WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('ACTIVE', 'CANCELLED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE holder_id IS NOT NULL
),
grouped AS (
    -- Group-level totals
    SELECT
        holder_id,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        COUNT(*) FILTER (WHERE status NOT IN ('LAPSED', 'EXPIRED')) AS n_status_not_in_lapsed_expired,
        MIN(premium) AS min_premium
    FROM scoped_policys
    GROUP BY 1, 2
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(min_premium) IGNORE NULLS OVER (ORDER BY min_premium DESC, holder_id, start_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_min_premium,
        SUM(min_premium) OVER (ORDER BY min_premium DESC, holder_id, start_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_premium,
        RANK() OVER (ORDER BY min_premium DESC, holder_id, start_date_month) AS min_premium_rank
    FROM grouped
),
qualified AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND YEAR(p.joined_on) = 2024
              AND p.holder_name IS NOT NULL
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT holder_id, start_date_month, metric_name, reading
FROM (
    SELECT holder_id, start_date_month, n_status_not_in_lapsed_expired, min_premium_rank
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (n_status_not_in_lapsed_expired, min_premium_rank)
) unp
