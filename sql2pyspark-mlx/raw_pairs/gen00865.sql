WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CASE WHEN unit_price >= 310 THEN 'High' WHEN unit_price >= 50 THEN 'Medium' ELSE 'Low' END AS unit_price_band
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (8, 23, 29)
)
SELECT
    id,
    unit_price_band,
    CUME_DIST() OVER (PARTITION BY order_id ORDER BY unit_price DESC) AS unit_price_cume_dist,
    AVG(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_avg_2,
    ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY quantity, id) AS event_seq,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id, product_id), 4) AS quantity_share,
    RANK() OVER (PARTITION BY product_id ORDER BY quantity NULLS LAST) AS quantity_rank
FROM valid
