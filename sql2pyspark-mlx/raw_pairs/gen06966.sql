WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(units) AS units_whole
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-03'
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.sale_date,
        s.units,
        s.revenue,
        s.units_whole,
        p.sqft
    FROM cleaned s
    JOIN stores p
        ON s.store_id = p.id
    WHERE p.sqft BETWEEN 14713 AND 16066
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(revenue, units) OVER w_ordered AS corr_revenue_units,
        CORR(units, revenue) OVER (PARTITION BY MONTH(sale_date), store_id) AS corr_units_revenue,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY store_id), 4) AS sqft_share,
        LEAD(units, 1, 0) OVER w_recent AS next_units
    FROM joined
    WINDOW w_ordered AS (PARTITION BY store_id), w_recent AS (PARTITION BY store_id ORDER BY sqft, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(units_whole, 0) AS units_whole_filled,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM with_windows
    WHERE sale_date >= DATE '2023-03-11'
      AND revenue < 2600
),
with_windows_store_sales AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        sale_date,
        revenue,
        corr_revenue_units,
        corr_units_revenue,
        sqft_share,
        units_whole_filled,
        days_since_sale_date,
        LEAD(corr_units_revenue) OVER (PARTITION BY store_id ORDER BY units_whole_filled, id) AS next_corr_units_revenue,
        LAG(days_since_sale_date) OVER (PARTITION BY store_id ORDER BY units_whole_filled, id) AS prev_days_since_sale_date,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units_whole_filled, id) AS row_seq,
        AVG(sqft_share) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date), store_id) AS sale_date_period_avg_sqft_share
    FROM filtered
)
SELECT
    id,
    sale_date,
    corr_revenue_units,
    corr_units_revenue,
    days_since_sale_date,
    row_seq,
    sale_date_period_avg_sqft_share
FROM with_windows_store_sales;
