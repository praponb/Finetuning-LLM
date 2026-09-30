WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATE_TRUNC('YEAR', end_date) AS end_date_year
    FROM policies
    WHERE (product IS NOT NULL OR start_date >= DATE '2024-06-19')
),
unique_rows AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        end_date_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium, id) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY holder_id, product) AS rows_in_holder_id_product,
        ROUND(premium - AVG(premium) OVER w_holder_id_product, 2) AS premium_vs_holder_id_product_avg
    FROM unique_rows
    WINDOW w_holder_id_product AS (PARTITION BY holder_id, product)
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date_year) AS end_date_year_month,
        SUM(premium) FILTER (WHERE product IS NOT NULL) AS premium_product_is_not_null,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        COUNT(*) AS row_count,
        SUM(CASE WHEN product = 'TRAVEL' THEN premium ELSE 0 END) AS premium_product_travel,
        COUNT(*) FILTER (WHERE product IN ('TRAVEL', 'HOME')) AS n_product_in_travel_home
    FROM with_trends
    GROUP BY 1
)
-- Final projection
SELECT
    end_date_year_month,
    premium_product_is_not_null,
    premium_product_travel,
    end_date_year_month + INTERVAL 15 MINUTES AS end_date_year_month_plus_15m,
    DATE_FORMAT(end_date_year_month, 'yyyy-MM') AS end_date_year_month_label
FROM summary
WHERE stddev_premium > 1250.0
