WITH filtered AS (
    SELECT *, CAST(quantity AS STRING) AS quantity_num
    FROM order_items
    WHERE product_id IS NOT NULL
),
enriched AS (
    SELECT
        s.*,
        b.discount,
        b.order_date
    FROM filtered s
    LEFT JOIN orders b
        ON s.order_id = b.id
),
kept_keys AS (
    SELECT product_id, id
    FROM enriched
    EXCEPT DISTINCT
    SELECT product_id, id
    FROM order_items
    WHERE unit_price > 320
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
),
enriched_order_items AS (
    SELECT
        id,
        order_id,
        product_id,
        discount,
        AVG(unit_price) OVER w_recent AS order_id_avg_unit_price,
        CORR(quantity, unit_price) OVER w_recent_2 AS corr_quantity_unit_price,
        DENSE_RANK() OVER w_recent_3 AS unit_price_dense_rank
    FROM kept_rows
    WINDOW w_recent AS (PARTITION BY order_id), w_recent_2 AS (PARTITION BY product_id, order_id), w_recent_3 AS (ORDER BY unit_price DESC NULLS LAST)
),
with_windows AS (
    SELECT
        *,
        ROUND(unit_price_dense_rank / SUM(unit_price_dense_rank) OVER (PARTITION BY order_id), 4) AS unit_price_dense_rank_share,
        DENSE_RANK() OVER (PARTITION BY product_id, order_id ORDER BY discount NULLS FIRST) AS discount_dense_rank
    FROM enriched_order_items
),
with_windows_order_items AS (
    SELECT
        *,
        RANK() OVER w_hist AS discount_rank,
        RANK() OVER (PARTITION BY product_id, order_id ORDER BY discount) AS discount_rank_2,
        MIN(corr_quantity_unit_price) OVER (PARTITION BY product_id) AS product_id_min_corr_quantity_unit_price
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY discount DESC NULLS FIRST)
),
with_dims AS (
    SELECT
        a.id,
        a.order_id,
        a.discount,
        a.order_id_avg_unit_price,
        a.corr_quantity_unit_price,
        a.discount_dense_rank,
        a.discount_rank,
        a.product_id_min_corr_quantity_unit_price,
        d.status,
        d.discount AS order_discount,
        d.amount
    FROM with_windows_order_items a
    LEFT JOIN orders d
        ON a.order_id = d.id
    WHERE d.discount BETWEEN 0.12 AND 0.3
)
SELECT
    order_id,
    order_id_avg_unit_price,
    corr_quantity_unit_price,
    discount_dense_rank,
    discount_rank,
    order_discount
FROM with_dims
ORDER BY discount_dense_rank, order_discount DESC
