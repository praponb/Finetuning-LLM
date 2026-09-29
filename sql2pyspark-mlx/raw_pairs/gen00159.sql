WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city, region ORDER BY opened_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_avg_5_prior,
        NTILE(3) OVER (PARTITION BY region ORDER BY sqft DESC, id ASC) AS sqft_bucket,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_avg_2
    FROM stores
    WHERE region = 'South'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft_avg_5_prior,
        sqft_bucket,
        SUM(sqft_bucket) OVER (PARTITION BY city ORDER BY sqft_avg_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_bucket,
        ROUND(sqft_avg_5_prior / SUM(sqft_avg_5_prior) OVER (PARTITION BY MONTH(opened_date), region), 4) AS sqft_avg_5_prior_share,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft_avg_5_prior DESC, id ASC) AS sqft_avg_5_prior_bucket
    FROM metrics
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        city,
        opened_date,
        sqft_avg_5_prior,
        cumulative_sqft_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft_bucket ASC NULLS FIRST, id ASC) AS rank_in_group
        FROM scored
    ) t
    WHERE rank_in_group = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
)
SELECT
    id,
    city,
    sqft_avg_5_prior,
    cumulative_sqft_bucket,
    CASE WHEN city RLIKE '^(Berlin|Bangkok)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS city_zone
FROM eligible
ORDER BY cumulative_sqft_bucket ASC NULLS LAST, city ASC
