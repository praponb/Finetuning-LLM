WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date IS NOT NULL
      AND premium >= 2096.6
),
holder_id_stats AS (
    -- Avg premium per holder_id
    SELECT holder_id, AVG(premium) AS avg_premium
    FROM scoped
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM scoped s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium > g.avg_premium
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        avg_premium,
        COUNT(*) OVER (PARTITION BY product, YEAR(end_date)) AS rows_in_product_end_date_year,
        ROUND(CUME_DIST() OVER (ORDER BY end_date), 3) AS end_date_cume_dist
    FROM vs_group
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        YEAR(end_date) AS end_date_year,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        COUNT(DISTINCT product) AS distinct_products,
        ROUND(SUM(end_date_cume_dist) / NULLIF(SUM(rows_in_product_end_date_year), 0), 3) AS end_date_cume_dist_to_rows_in_product_end_date_y,
        DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
        ROUND(SUM(avg_premium) / NULLIF(SUM(rows_in_product_end_date_year), 0), 3) AS avg_premium_to_rows_in_product_end_date_year_rat
    FROM sequenced
    GROUP BY 1, 2, 3
    HAVING SUM(end_date_cume_dist) > 0.1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY start_date_span_days DESC, product, start_date_ym, end_date_year) AS rn,
        LAG(start_date_span_days, 3) OVER (ORDER BY start_date_span_days DESC, product, start_date_ym, end_date_year) AS prev3_start_date_span_days,
        COUNT(*) OVER (PARTITION BY product, distinct_products) AS rows_in_product_distinct_products,
        NTILE(10) OVER w_all_by_start_date_span_days_2 AS start_date_span_days_decile,
        ROUND(distinct_products / NULLIF(SUM(distinct_products) OVER w_product_distinct_products, 0), 4) AS distinct_products_share_of_product_distinct_prod
    FROM summary
    WINDOW w_all_by_start_date_span_days_2 AS (ORDER BY start_date_span_days, product, start_date_ym, end_date_year), w_product_distinct_products AS (PARTITION BY product, distinct_products)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(prev3_start_date_span_days) OVER (PARTITION BY distinct_products ORDER BY rows_in_product_distinct_products DESC, product, start_date_ym, end_date_year ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS prev3_start_date_span_days_avg_2,
        LEAD(prev3_start_date_span_days, 3) OVER (PARTITION BY distinct_products ORDER BY rows_in_product_distinct_products DESC, product, start_date_ym, end_date_year) AS next3_prev3_start_date_span_days,
        LAST_VALUE(start_date_span_days) IGNORE NULLS OVER (PARTITION BY distinct_products ORDER BY rows_in_product_distinct_products DESC, product, start_date_ym, end_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date_span_days,
        MAX(start_date_span_days) OVER (PARTITION BY distinct_products ORDER BY rows_in_product_distinct_products DESC, product, start_date_ym, end_date_year ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS start_date_span_days_max_7
    FROM windowed
),
annotated AS (
    -- Add calculated columns
    SELECT *, ARRAY_JOIN(ARRAY_SORT(product_list), ',') AS product_list_text
    FROM with_trends
)
-- Final projection
SELECT
    product,
    start_date_ym,
    end_date_year,
    product_list,
    distinct_products,
    end_date_cume_dist_to_rows_in_product_end_date_y,
    start_date_span_days,
    avg_premium_to_rows_in_product_end_date_year_rat,
    rn,
    rows_in_product_distinct_products,
    prev3_start_date_span_days_avg_2,
    next3_prev3_start_date_span_days,
    last_known_start_date_span_days,
    start_date_span_days_max_7,
    product_list_text,
    CONCAT(start_date_ym, '#', CAST(rn AS STRING)) AS start_date_ym_ref,
    ABS(start_date_span_days - 455) AS start_date_span_days_distance
FROM annotated
ORDER BY end_date_cume_dist_to_rows_in_product_end_date_y
