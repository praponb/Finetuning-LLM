WITH cleaned AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE sale_date > DATE '2023-06-10'
      AND product_id IN (1, 3, 4, 5)
),
enriched AS (
    SELECT
        e.*,
        b.sqft,
        b.city,
        b.region
    FROM cleaned e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
cleaned_store_sales AS (
    SELECT
        *,
        COALESCE(region, 'N/A') AS region_filled,
        COALESCE(region, 'N/A') AS resolved_region
    FROM enriched
    WHERE sale_date > TO_DATE('2023-02-13')
      AND region IS NOT NULL
),
enriched_store_sales AS (
    SELECT
        *,
        SUM(sqft) OVER w_recent AS running_sum_sqft,
        AVG(sqft) OVER w_recent AS running_avg_sqft,
        MAX(sqft) OVER w_recent AS running_max_sqft
    FROM cleaned_store_sales
    WINDOW w_recent AS (PARTITION BY city ORDER BY sale_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    SELECT *, GREATEST(revenue, units) AS greatest_revenue_units
    FROM enriched_store_sales
    WHERE sale_date >= DATE '2023-06-10'
),
matched_keys AS (
    SELECT store_id, id
    FROM valid
    EXCEPT
    SELECT store_id, id
    FROM store_sales
    WHERE YEAR(sale_date) = 2025
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.store_id = k.store_id AND s.id = k.id
),
cleaned_store_sales_2 AS (
    SELECT
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        sale_date_month_end,
        sqft,
        city,
        region_filled,
        resolved_region,
        running_sum_sqft,
        running_avg_sqft,
        running_max_sqft,
        greatest_revenue_units,
        revenue + running_avg_sqft AS revenue_plus_running_avg_sqft,
        ROUND(greatest_revenue_units, 0) AS greatest_revenue_units_rounded,
        COALESCE(city, 'NONE') AS resolved_city
    FROM kept_rows
    WHERE region IS NOT NULL
)
SELECT
    store_id,
    product_id,
    sale_date,
    revenue,
    sale_date_month_end,
    region_filled,
    running_sum_sqft,
    running_avg_sqft,
    running_max_sqft
FROM cleaned_store_sales_2;
