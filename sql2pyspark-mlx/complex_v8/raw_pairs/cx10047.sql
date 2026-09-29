WITH recent AS (
    -- Filter policies
    SELECT *
    FROM policies
    WHERE (status = 'CANCELLED' OR end_date BETWEEN DATE '2025-06-08' AND DATE '2026-12-05')
),
with_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        p.region AS holder_region,
        p.holder_name,
        p.segment AS holder_segment
    FROM recent s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_start_date
    FROM with_details s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_end_date AS rn,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        NTILE(5) OVER (ORDER BY end_date DESC, id) AS end_date_quintile,
        ROW_NUMBER() OVER w_first_policy_start_date_mont AS rn_2,
        RANK() OVER (PARTITION BY MONTH(first_policy_start_date), product ORDER BY premium, id) AS premium_rank,
        DENSE_RANK() OVER (ORDER BY end_date DESC, id) AS end_date_dense_rank
    FROM summarized_rows
    WINDOW w_all_by_end_date AS (ORDER BY end_date DESC, id), w_first_policy_start_date_mont AS (PARTITION BY MONTH(first_policy_start_date), product ORDER BY premium, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, end_date_dense_rank, rn
    FROM with_trends
) src
UNPIVOT (
    value FOR metric_name IN (end_date_dense_rank, rn)
) unp
