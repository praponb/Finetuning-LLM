WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE premium BETWEEN 1042.8 AND 3521.8
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        premium - LAG(premium) OVER (PARTITION BY YEAR(end_date), product ORDER BY start_date, id) AS premium_change,
        premium - LAG(premium) OVER w_product_status_by_premium AS premium_change_2,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status), 2) AS premium_vs_status_avg,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        MAX(premium) OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far
    FROM filtered
    WINDOW w_product_status_by_premium AS (PARTITION BY product, status ORDER BY premium, id)
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM ranked s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
        )
      AND s.premium < 3817.5
),
with_activity_2 AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.region IS NOT NULL
              AND p.holder_name IS NOT NULL
        )
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_activity_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id >= 4
              AND c.status IS NOT NULL
        )
)
SELECT
    id,
    holder_id,
    premium,
    end_date,
    status,
    premium_change,
    premium_change_2,
    premium_vs_status_avg,
    rows_in_status,
    ROUND(premium_vs_status_avg / NULLIF(premium, 0), 3) AS premium_vs_status_avg_per_premium
FROM qualified
ORDER BY holder_id
