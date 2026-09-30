WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY units DESC) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM ranked
    EXCEPT
    SELECT store_id
    FROM store_sales
    WHERE YEAR(sale_date) = 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.store_id = k.store_id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    product_id,
    BIT_OR(units) AS units_bit_or,
    COUNT(*) AS cnt,
    CORR(revenue, units) AS corr_revenue_units
FROM verified
GROUP BY DATE_TRUNC('WEEK', sale_date), product_id
ORDER BY corr_revenue_units
