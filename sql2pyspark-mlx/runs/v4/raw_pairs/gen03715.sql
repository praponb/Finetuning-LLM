WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(status) AS status_length
    FROM orders
    WHERE store_id IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM cleaned src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(shipping_fee) OVER (PARTITION BY channel ORDER BY order_date, id, promo_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_3_prior,
        SUM(status_length) OVER (ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_length,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY order_date, id, promo_id) AS seq_no
    FROM matched
),
cleaned_orders AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        status_length,
        promo_id,
        promo_code,
        discount_pct,
        shipping_fee_avg_3_prior,
        seq_no,
        ROUND(promo_id / seq_no, 2) AS promo_id_per_seq_no,
        COALESCE(promo_code, 'N/A') AS promo_code_clean
    FROM scored
    WHERE order_date > DATE '2023-07-20'
      AND promo_code IS NOT NULL
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.discount,
    s.status_length,
    s.promo_id,
    s.promo_code,
    s.shipping_fee_avg_3_prior,
    s.seq_no,
    (SELECT MAX(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS max_inventory_quantity
FROM cleaned_orders s
ORDER BY seq_no NULLS LAST;
