WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM store_sales
    EXCEPT ALL
    SELECT id, store_id
    FROM store_sales
    WHERE product_id = 8
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        ADD_MONTHS(sale_date, 6) AS sale_date_plus_months,
        DAY(sale_date) AS sale_date_day
    FROM verified
    WHERE sale_date >= TO_DATE('2023-06-24')
      AND revenue < 2800
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.region,
        p.store_name,
        p.city
    FROM cleaned e
    INNER JOIN stores p
        ON e.store_id = p.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY units NULLS FIRST) AS units_percent_rank,
        MIN(sale_date_day) OVER (PARTITION BY city) AS city_min_sale_date_day,
        RANK() OVER w_running AS sale_date_day_rank,
        ROUND(units / SUM(units) OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date_plus_months)), 4) AS units_share,
        PERCENT_RANK() OVER (ORDER BY units NULLS FIRST) AS units_percent_rank_2
    FROM with_dims
    WINDOW w_running AS (PARTITION BY store_id ORDER BY sale_date_day NULLS LAST)
),
matched_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM enriched
    EXCEPT ALL
    SELECT product_id
    FROM store_sales
    WHERE revenue <= 3700
),
verified_store_sales AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.product_id = k.product_id
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT
        src.store_id,
        src.product_id,
        src.sale_date,
        src.store_name,
        src.units_percent_rank,
        src.units_share,
        d.category
    FROM verified_store_sales src
    LEFT JOIN products d
        ON src.product_id = d.id
)
SELECT
    s.store_id,
    s.units_percent_rank,
    s.units_share,
    (SELECT MIN(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS min_inventory_quantity
FROM enriched_store_sales s;
