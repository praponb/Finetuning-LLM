WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        CASE WHEN status IN ('CANCELLED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        CASE WHEN premium >= 3465.4 THEN 'large' WHEN premium >= 2111.3 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
    WHERE premium <= 3544.1
      AND holder_id >= 18
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        status_group,
        COUNT(DISTINCT status) AS distinct_status,
        SUM(premium_bucket) FILTER (WHERE product = 'HOME') AS premium_bucket_product_home,
        MAX(end_date) AS last_end_date,
        MIN(premium) AS min_premium,
        MIN(end_date) AS first_end_date
    FROM filtered
    GROUP BY 1, 2
    HAVING AVG(premium) > 1541.8
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE distinct_status > (SELECT MIN(distinct_status) FROM per_group)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(premium_bucket_product_home, 2) OVER (PARTITION BY status_group ORDER BY premium_bucket_product_home DESC NULLS FIRST, end_date_month, status_group ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_bucket_product_home,
        LAG(first_end_date) OVER (PARTITION BY status_group ORDER BY premium_bucket_product_home DESC NULLS FIRST, end_date_month, status_group) AS prev_first_end_date,
        ROUND(CUME_DIST() OVER w_status_group_by_premium_buck_3, 3) AS premium_bucket_product_home_cume_dist,
        MIN(distinct_status) OVER (PARTITION BY YEAR(last_end_date), distinct_status) AS last_end_date_year_distinct_status_min_distinct_
    FROM screened
    WINDOW w_status_group_by_premium_buck_3 AS (PARTITION BY status_group ORDER BY premium_bucket_product_home DESC NULLS FIRST)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE distinct_status > (SELECT AVG(distinct_status) FROM ranked)
)
SELECT
    end_date_month,
    status_group,
    distinct_status,
    premium_bucket_product_home,
    min_premium,
    first_end_date,
    ROUND(distinct_status / NULLIF(min_premium, 0), 3) AS distinct_status_per_min_premium
FROM without_matches
ORDER BY min_premium, first_end_date DESC, end_date_month, status_group
LIMIT 20
