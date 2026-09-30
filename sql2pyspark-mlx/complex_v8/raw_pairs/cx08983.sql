WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label
    FROM policies
    WHERE status = 'EXPIRED'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at
    FROM valid_policys s
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        start_date_label,
        first_claim_reported_at,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM with_stats
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_status_by_first_claim_report AS first_claim_reported_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS rn
    FROM labeled
    WINDOW w_status_by_first_claim_report AS (PARTITION BY status ORDER BY first_claim_reported_at DESC NULLS FIRST, id)
),
with_stats_2 AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.holder_id) AS avg_policy_premium,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count
    FROM sequenced s
),
latest_per_key AS (
    -- Keep the latest row per product, status
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        start_date_label,
        first_claim_reported_at,
        status_prefix,
        first_claim_reported_at_dense_rank,
        rn,
        avg_policy_premium,
        policy_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY first_claim_reported_at_dense_rank DESC, id DESC) AS row_num
        FROM with_stats_2
    ) ranked
    WHERE row_num = 1
),
deduped AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        start_date_label,
        first_claim_reported_at,
        status_prefix,
        first_claim_reported_at_dense_rank,
        rn,
        avg_policy_premium,
        policy_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY avg_policy_premium DESC, id DESC) AS rn_2
        FROM latest_per_key
    ) ranked
    WHERE rn_2 = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, amount
FROM (
    SELECT id, policy_count, first_claim_reported_at_dense_rank, rn
    FROM deduped
) src
UNPIVOT (
    amount FOR metric_name IN (policy_count AS policy_count_v, first_claim_reported_at_dense_rank AS first_claim_reported_at_dense_rank_v, rn AS rn_v)
) unp
