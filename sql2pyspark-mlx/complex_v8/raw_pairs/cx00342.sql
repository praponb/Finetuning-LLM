WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id <= 11
      AND start_date < DATE '2025-03-23'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        MIN(end_date) AS first_policy_end_date,
        COUNT(*) AS policy_count,
        COUNT(*) AS policy_count_2
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        p.first_policy_end_date,
        COALESCE(p.policy_count, 0) AS policy_count,
        COALESCE(p.policy_count_2, 0) AS policy_count_2
    FROM scoped_policys s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
unique_rows AS (
    -- Keep the latest row per status, holder_id
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        first_policy_end_date,
        policy_count,
        policy_count_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY policy_count_2 DESC, id DESC) AS recency_rank
        FROM profiled
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        policy_count_2 - LAG(policy_count_2) OVER (ORDER BY first_policy_end_date DESC, id) AS policy_count_2_change,
        ROUND(policy_count_2 - AVG(policy_count_2) OVER (PARTITION BY holder_id), 2) AS policy_count_2_vs_holder_id_avg,
        LEAD(end_date, 2) OVER (PARTITION BY status, policy_count_2 ORDER BY start_date, id) AS next2_end_date
    FROM unique_rows
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.policy_count,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status
    FROM windowed s
    JOIN claims c ON c.policy_id = s.id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        UPPER(TRIM(claim_status)) AS claim_status_clean
    FROM detail_rows
    WHERE claim_approved_amount <= 14668.0
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.policy_count,
        s.claim_id,
        c.id AS claim_id_2,
        c.claim_type,
        c.claim_date
    FROM enriched s
    LEFT JOIN claims c ON c.policy_id = s.id
)
SELECT
    id,
    policy_count,
    claim_id,
    claim_type,
    claim_date,
    DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
FROM exploded_detail
ORDER BY policy_count DESC
