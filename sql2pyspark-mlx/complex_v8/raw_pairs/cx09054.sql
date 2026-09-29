WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE status IS NOT NULL
      AND start_date IS NOT NULL
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM prepared s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.adjuster_id IS NOT NULL
        )
      AND s.premium BETWEEN 1541.8 AND 4688.7
),
holder_id_stats AS (
    -- Max premium per holder_id
    SELECT holder_id, MAX(premium) AS max_premium
    FROM screened
    GROUP BY holder_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM screened s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium = g.max_premium
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (ORDER BY end_date, id) AS end_date_dense_rank,
        LAG(premium, 2) OVER (ORDER BY end_date, id) AS prev2_premium,
        RANK() OVER w_all_by_end_date AS end_date_rank,
        ROUND(premium_minus_max / NULLIF(SUM(premium_minus_max) OVER (PARTITION BY product, YEAR(start_date)), 0), 4) AS premium_minus_max_share_of_product_start_date_ye,
        NTH_VALUE(start_date, 2) OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM outliers
    WINDOW w_all_by_end_date AS (ORDER BY end_date, id)
)
-- Subtotals with CUBE
SELECT
    end_date_dense_rank,
    COALESCE(status_group, 'ALL') AS status_group,
    holder_id,
    MIN(end_date) AS first_end_date
FROM metrics
GROUP BY CUBE(end_date_dense_rank, status_group, holder_id)
