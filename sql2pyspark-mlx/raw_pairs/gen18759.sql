WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CASE WHEN units >= 38 THEN 'High' WHEN units >= 12 THEN 'Medium' ELSE 'Low' END AS units_tier,
        DATE_SUB(sale_date, 45) AS sale_date_minus_45d
    FROM store_sales
    WHERE sale_date > DATE '2023-06-27'
      AND product_id IN (1, 3, 8, 10)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.units,
        src.revenue,
        src.units_tier,
        b.sqft,
        b.city,
        b.region
    FROM valid src
    INNER JOIN stores b
        ON src.store_id = b.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        revenue,
        units_tier,
        LAG(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id) AS prev_revenue,
        LEAD(revenue) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_revenue,
        LAG(units) OVER (PARTITION BY store_id ORDER BY revenue, id) AS prev_units,
        LEAD(units) OVER (PARTITION BY store_id ORDER BY revenue, id) AS next_units,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST) AS sqft_dense_rank
    FROM with_dims
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        revenue,
        units_tier,
        prev_revenue,
        next_revenue,
        prev_units,
        next_units,
        seq_no,
        sqft_dense_rank,
        GREATEST(next_revenue, revenue) AS greatest_next_revenue_revenue
    FROM enriched
    WHERE seq_no BETWEEN 5 AND 6
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(sqft_dense_rank) OVER w_hist AS prev_sqft_dense_rank,
        LEAD(sqft_dense_rank) OVER w_hist AS next_sqft_dense_rank,
        LAG(next_units) OVER w_hist AS prev_next_units,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM base
    WINDOW w_hist AS (ORDER BY revenue, id)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_units >= 44 THEN 'LARGE' ELSE 'SMALL' END AS prev_units_tier,
    ROUND(AVG(greatest_next_revenue_revenue), 2) AS avg_greatest_next_revenue_revenue,
    APPROX_PERCENTILE(event_seq, 0.5) AS event_seq_p50,
    PERCENTILE_APPROX(prev_revenue, ARRAY(0.25, 0.5, 0.75)) AS prev_revenue_quartiles
FROM with_windows
GROUP BY CASE WHEN prev_units >= 44 THEN 'LARGE' ELSE 'SMALL' END;
