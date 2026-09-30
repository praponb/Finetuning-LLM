WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
    WHERE premium >= 2111.3
      AND start_date < DATE '2025-05-02'
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        YEAR(end_date) AS end_date_year,
        product,
        ROUND(100 * COUNT(CASE WHEN product = 'HEALTH' THEN 1 END) / COUNT(*), 1) AS pct_product_health,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(*) FILTER (WHERE status IN ('ACTIVE', 'CANCELLED', 'LAPSED')) AS n_status_in_active_cancelled_l,
        MAX(end_date) AS last_end_date
    FROM recent_policys
    WHERE holder_id <= 11
    GROUP BY 1, 2, 3
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM grouped
    WHERE n_status_in_active_cancelled_l > (SELECT MIN(n_status_in_active_cancelled_l) FROM grouped)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(last_end_date, 'yyyy-MM') AS last_end_date_label,
        distinct_holder_ids + end_date_year AS distinct_holder_ids_plus_end_date_year,
        COALESCE(distinct_holder_ids, 0) AS distinct_holder_ids_filled
    FROM without_matches
    WHERE n_status_in_active_cancelled_l >= 2
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROUND(pct_product_health - AVG(pct_product_health) OVER w_product, 2) AS pct_product_health_vs_product_avg,
        ROUND(AVG(distinct_holder_ids_filled) OVER (ORDER BY n_status_in_active_cancelled_l DESC, status, end_date_year, product ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS distinct_holder_ids_filled_avg_2_prior
    FROM derived
    WINDOW w_product AS (PARTITION BY product)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE pct_product_health_vs_product_avg > (SELECT AVG(pct_product_health_vs_product_avg) FROM with_trends)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY end_date_year, distinct_holder_ids ORDER BY pct_product_health DESC), 3) AS pct_product_health_percent_rank,
        MIN(rows_in_product) OVER w_distinct_holder_ids AS distinct_holder_ids_min_rows_in_product
    FROM with_activity
    WINDOW w_distinct_holder_ids AS (PARTITION BY distinct_holder_ids)
)
-- Final projection
SELECT
    status,
    end_date_year,
    pct_product_health,
    distinct_holder_ids,
    n_status_in_active_cancelled_l,
    last_end_date_label,
    distinct_holder_ids_plus_end_date_year,
    distinct_holder_ids_filled,
    rows_in_product,
    pct_product_health_vs_product_avg,
    pct_product_health_percent_rank,
    distinct_holder_ids_min_rows_in_product,
    LEAST(rows_in_product, pct_product_health) AS least_rows_in_product_pct_product_health,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM metrics
WHERE last_end_date_label IS NOT NULL
