WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) / 30 months_since_sale_date,
        ROUND(units, 0) units_rounded
    FROM store_sales
    WHERE revenue < 2200
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.revenue,
        a.months_since_sale_date,
        a.units_rounded,
        d.sqft,
        d.store_name
    FROM valid a
    INNER JOIN stores d
        ON a.store_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        revenue,
        sqft,
        MIN(revenue) OVER w_hist last3_min_revenue,
        AVG(revenue) OVER w_hist last3_avg_revenue,
        MAX(revenue) OVER w_hist last3_max_revenue
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
valid_store_sales AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(sqft, 0) sqft_filled
    FROM with_windows
    WHERE last3_max_revenue >= 3200
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        last3_avg_revenue,
        last3_max_revenue,
        NTILE(4) OVER (ORDER BY sqft, id) sqft_bucket,
        LAST_VALUE(last3_avg_revenue, TRUE) OVER w_part last_known_last3_avg_revenue,
        FIRST_VALUE(sqft_filled, TRUE) OVER w_part first_known_sqft_filled
    FROM valid_store_sales
    WINDOW w_part AS (ORDER BY last3_min_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY last_known_last3_avg_revenue, id) event_seq,
        LEAD(last3_avg_revenue, 2) OVER (ORDER BY sqft_bucket, id) next_last3_avg_revenue,
        ROW_NUMBER() OVER w_recent row_seq
    FROM metrics
    WHERE last_known_last3_avg_revenue > 3400
    WINDOW w_recent AS (ORDER BY first_known_sqft_filled DESC, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        last_known_last3_avg_revenue,
        last3_max_revenue,
        last3_avg_revenue
    FROM with_windows_store_sales
) src
UNPIVOT (
    metric_value FOR kpi IN (last_known_last3_avg_revenue AS LAST_KNOWN_LAST3_AVG_REVENUE, last3_max_revenue, last3_avg_revenue AS LAST3_AVG_REVENUE_VAL)
)
