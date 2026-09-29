WITH valid AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(quantity, 0) AS quantity_filled
    FROM order_items
    WHERE order_id = 37
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC NULLS FIRST) AS rn
        FROM valid
    ) x
    WHERE rn = 1
),
enriched AS (
    SELECT
        *,
        CORR(unit_price, quantity) OVER (PARTITION BY product_id, order_id) AS corr_unit_price_quantity,
        AVG(quantity_filled) OVER (ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS quantity_filled_avg_5,
        AVG(quantity) OVER (PARTITION BY order_id, product_id ORDER BY quantity_filled, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior,
        AVG(unit_price) OVER (PARTITION BY product_id) AS product_id_avg_unit_price
    FROM ranked
),
ranked_order_items AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity_filled_avg_5) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
joined AS (
    SELECT
        s.*,
        d.product_name,
        d.stock_qty
    FROM ranked_order_items s
    LEFT JOIN products d
        ON s.product_id = d.id
)
SELECT
    product_id,
    CASE WHEN stock_qty >= 398 THEN 'LARGE' ELSE 'SMALL' END AS stock_qty_tier,
    BIT_AND(quantity) AS quantity_bit_and,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(quantity_filled, ARRAY(0.25, 0.5, 0.75)) AS quantity_filled_quartiles,
    CORR(quantity, unit_price) AS corr_quantity_unit_price
FROM joined
GROUP BY product_id, stock_qty_tier
