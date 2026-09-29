WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        LAST_DAY(last_restock) last_restock_month_end
    FROM inventory
    WHERE last_restock > DATE '2023-05-14'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(quantity) OVER w_recent last3_count_quantity,
        SUM(quantity) OVER w_recent last3_sum_quantity,
        AVG(quantity) OVER w_recent last3_avg_quantity,
        MAX(quantity) OVER w_recent last3_max_quantity,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity
    FROM valid
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY last_restock_month_end, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        a.last_restock_month_end,
        a.last3_sum_quantity,
        a.cumulative_quantity,
        b.city
    FROM metrics a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.store_name,
        b.city AS store_city,
        b.region
    FROM with_dims a
    INNER JOIN stores b
        ON a.store_id = b.id
    WHERE b.sqft BETWEEN 6756 AND 14764
),
grouped AS (
    -- Grouped metrics
    SELECT
        product_id,
        BIT_AND(quantity) AS quantity_bit_and,
        COUNT(CASE WHEN store_city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
        SUM(CASE WHEN store_city = 'Berlin' THEN cumulative_quantity ELSE 0 END) AS berlin_cumulative_quantity,
        AVG(cumulative_quantity) AS avg_cumulative_quantity,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(cumulative_quantity, ARRAY(0.25, 0.5, 0.75)) AS cumulative_quantity_quartiles
    FROM enriched
    GROUP BY 1
)
SELECT
    s.product_id,
    s.chiang_mai_count,
    s.berlin_cumulative_quantity,
    s.cumulative_quantity_quartiles,
    (SELECT SUM(i2.quantity) FROM inventory i2 WHERE i2.product_id = s.product_id) AS sum_inventory_quantity
FROM grouped s
