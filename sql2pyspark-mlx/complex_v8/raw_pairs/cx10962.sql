WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        CASE WHEN product IN ('HEALTH', 'LIFE') THEN 'group_a' WHEN product = 'TRAVEL' THEN 'group_b' ELSE 'other' END AS product_group,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM policies
    WHERE (start_date >= DATE '2024-07-19' OR product = 'TRAVEL')
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_approved_amount
    FROM recent_policys s
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN product_group IN ('other', 'group_a') THEN 'group_a' WHEN product_group = 'group_b' THEN 'group_b' ELSE 'other' END AS product_group_group
    FROM summarized_rows
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        start_date,
        end_date,
        product_or_unknown,
        product_group,
        product_ref,
        product_group_group,
        SUM(premium) OVER (ORDER BY start_date ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS premium_sum_3_prior,
        ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY min_claim_approved_amount DESC, id) AS seq_no
    FROM derived
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM windowed s
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        start_date,
        end_date,
        product_or_unknown,
        product_group,
        product_ref,
        product_group_group,
        premium_sum_3_prior,
        first_claim_reported_at,
        claim_count
    FROM with_stats
    WHERE seq_no <= 5
      AND end_date >= DATE '2026-02-16'
),
with_stats_2 AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count_2,
        (SELECT COUNT(CASE WHEN c.status NOT IN ('IN_REVIEW', 'REJECTED') THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_status_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at_2
    FROM top_rows s
)
-- Final projection
SELECT
    id,
    end_date,
    product_or_unknown,
    product_group,
    product_group_group,
    premium_sum_3_prior,
    claim_count,
    claim_count_2,
    claim_status_count,
    first_claim_reported_at_2
FROM with_stats_2
WHERE first_claim_reported_at_2 < TIMESTAMP '2025-02-13 09:00:00'
ORDER BY claim_count_2
