WITH scored AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        PERCENT_RANK() OVER (PARTITION BY product_id, order_id ORDER BY unit_price DESC NULLS LAST) AS unit_price_percent_rank,
        SUM(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_unit_price,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY product_id), 4) AS quantity_share,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price
    FROM order_items
),
enriched AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        unit_price_percent_rank,
        cumulative_unit_price,
        quantity_share,
        corr_quantity_unit_price,
        CORR(quantity, unit_price) OVER w_part AS corr_quantity_unit_price_2,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST) AS quantity_dense_rank
    FROM scored
    WINDOW w_part AS (PARTITION BY order_id)
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER w_grp AS quantity_cume_dist,
        PERCENT_RANK() OVER (ORDER BY unit_price_percent_rank DESC NULLS FIRST) AS unit_price_percent_rank_percent_rank,
        LAST_VALUE(quantity_dense_rank, TRUE) OVER (ORDER BY unit_price_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity_dense_rank,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        LAG(quantity_share, 2, 0) OVER (PARTITION BY order_id, product_id ORDER BY quantity_dense_rank, id) AS prev_quantity_share
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY quantity DESC NULLS LAST)
)
SELECT
    id,
    order_id,
    quantity,
    cumulative_unit_price,
    quantity_share,
    corr_quantity_unit_price,
    unit_price_percent_rank_percent_rank,
    unit_price_share
FROM with_windows;
