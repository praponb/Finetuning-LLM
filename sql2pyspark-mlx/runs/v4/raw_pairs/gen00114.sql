WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(revenue AS INT) AS revenue_int,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM store_sales
    WHERE units < 24
),
enriched AS (
    -- Attach products attributes
    SELECT a.*, b.stock_qty
    FROM cleaned a
    LEFT JOIN products b
        ON a.product_id = b.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY stock_qty DESC, id) = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        d.city,
        d.region
    FROM deduped e
    JOIN stores d
        ON e.store_id = d.id
),
cleaned_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        revenue_int,
        sale_date_month_end,
        stock_qty,
        city,
        region,
        revenue_int - stock_qty AS revenue_int_minus_stock_qty,
        GREATEST(revenue, units) AS greatest_revenue_units
    FROM with_dims
    WHERE sale_date >= DATE '2023-04-24'
      AND city IS NOT NULL
      AND units = 26
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        region,
        COUNT_IF(city = 'New York') AS new_york_events,
        ROUND(SUM(greatest_revenue_units) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS greatest_revenue_units_per_tokyo,
        COUNT_IF(city = 'Singapore') AS singapore_events
    FROM cleaned_store_sales
    GROUP BY region
)
SELECT
    region,
    new_york_events,
    greatest_revenue_units_per_tokyo,
    singapore_events
FROM summary
ORDER BY region DESC NULLS LAST, new_york_events
LIMIT 20;
