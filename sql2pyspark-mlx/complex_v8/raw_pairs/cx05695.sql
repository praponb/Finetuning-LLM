WITH valid AS (
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
    WHERE premium >= 2111.3
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        product,
        ROUND(100 * COUNT(CASE WHEN status IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_status_is_not_null,
        SUM(premium) AS total_premium,
        MIN(premium) AS min_premium
    FROM valid
    GROUP BY 1, 2
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        start_date_month,
        product,
        pct_status_is_not_null,
        total_premium,
        FIRST_VALUE(total_premium) OVER (PARTITION BY product ORDER BY min_premium NULLS LAST, start_date_month, product ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_total_premium,
        min_premium - LAG(min_premium) OVER (PARTITION BY product ORDER BY min_premium NULLS LAST, start_date_month, product) AS min_premium_change,
        MIN(pct_status_is_not_null) OVER (PARTITION BY product) AS product_min_pct_status_is_not_null,
        RANK() OVER (PARTITION BY product ORDER BY min_premium NULLS LAST, start_date_month, product) AS min_premium_rank
    FROM totals
),
labeled AS (
    -- Derive helper columns
    SELECT *, start_date_month + INTERVAL 30 MINUTES AS start_date_month_plus_30m
    FROM ranked
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE product_min_pct_status_is_not_null > (SELECT AVG(product_min_pct_status_is_not_null) FROM labeled)
),
annotated AS (
    -- Label and score rows
    SELECT
        start_date_month,
        product,
        pct_status_is_not_null,
        total_premium,
        min_premium_change,
        product_min_pct_status_is_not_null,
        min_premium_rank,
        start_date_month + INTERVAL 7 DAYS AS start_date_month_plus_7d,
        CASE WHEN min_premium_rank >= 8 THEN 'HIGH' WHEN min_premium_rank >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS min_premium_rank_tier,
        LEAST(min_premium_rank, first_total_premium) AS least_min_premium_rank_first_total_premium
    FROM filtered
)
-- Final projection
SELECT
    start_date_month,
    product,
    pct_status_is_not_null,
    total_premium,
    min_premium_change,
    product_min_pct_status_is_not_null,
    min_premium_rank,
    start_date_month_plus_7d,
    min_premium_rank_tier,
    least_min_premium_rank_first_total_premium,
    ROUND(total_premium / NULLIF(pct_status_is_not_null, 0), 3) AS total_premium_per_pct_status_is_not_null
FROM annotated
ORDER BY min_premium_rank_tier, least_min_premium_rank_first_total_premium DESC NULLS FIRST, start_date_month, product
LIMIT 5
