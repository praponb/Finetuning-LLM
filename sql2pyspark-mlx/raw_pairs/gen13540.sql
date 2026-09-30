WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-21'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT product_id
    FROM store_sales
    WHERE store_id = 5
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.product_id = k.product_id
),
joined AS (
    -- Attach products attributes
    SELECT
        src.id,
        src.product_id,
        src.sale_date,
        src.units,
        d.cost,
        d.product_name,
        d.brand
    FROM kept_rows src
    LEFT JOIN products d
        ON src.product_id = d.id
),
cleaned_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LAST_DAY(sale_date) AS sale_date_month_end,
        COALESCE(brand, 'NONE') AS brand_filled
    FROM joined
    WHERE sale_date > DATE '2023-09-24'
)
-- Aggregate with conditional metrics
SELECT
    product_id,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    MAX(cost) AS max_cost,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    APPROX_PERCENTILE(cost, 0.95) AS cost_p95,
    ROUND(AVG(units), 2) AS avg_units,
    BIT_AND(units) AS units_bit_and
FROM cleaned_store_sales
GROUP BY product_id, DATE_TRUNC('WEEK', sale_date)
ORDER BY umbrella_count DESC NULLS LAST
