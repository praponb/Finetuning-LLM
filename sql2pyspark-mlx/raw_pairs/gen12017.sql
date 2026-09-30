WITH filtered AS (
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CAST(unit_price AS INT) unit_price_int,
        ROUND(quantity, 2) quantity_rounded
    FROM order_items
    WHERE order_id IN (2, 3, 28, 30)
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered quantity_rounded_percent_rank,
        SUM(quantity_rounded) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity_rounded,
        ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id), 4) quantity_share,
        AVG(unit_price) OVER (ORDER BY quantity_rounded, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) unit_price_avg_6_prior,
        SUM(quantity_rounded) OVER (ORDER BY unit_price_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_quantity_rounded_2
    FROM filtered
    WHERE quantity > 6
    WINDOW w_ordered AS (ORDER BY quantity_rounded DESC)
),
joined AS (
    SELECT
        src.*,
        p.discount,
        p.channel,
        p.amount
    FROM scored src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
with_windows AS (
    SELECT
        id,
        order_id,
        product_id,
        unit_price_int,
        quantity_rounded,
        quantity_share,
        cumulative_quantity_rounded_2,
        discount,
        ROW_NUMBER() OVER (ORDER BY unit_price_avg_6_prior, id) event_seq,
        ROW_NUMBER() OVER (ORDER BY quantity_rounded_percent_rank, id) row_seq,
        LAST_VALUE(quantity_rounded_percent_rank) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_quantity_rounded_percent_rank,
        NTILE(5) OVER (PARTITION BY channel ORDER BY unit_price DESC, id) unit_price_bucket,
        SUM(discount) OVER (PARTITION BY product_id ORDER BY unit_price_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount
    FROM joined
)
SELECT
    id,
    product_id,
    quantity_share,
    cumulative_quantity_rounded_2,
    discount,
    LAG(cumulative_discount) OVER w_recent prev_cumulative_discount,
    LEAD(last_known_quantity_rounded_percent_rank) OVER w_recent next_last_known_quantity_rounded_percent_rank,
    LAG(last_known_quantity_rounded_percent_rank) OVER w_recent prev_last_known_quantity_rounded_percent_rank,
    ROW_NUMBER() OVER w_recent row_seq_2,
    ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY order_id, product_id), 4) row_seq_share
FROM with_windows
WINDOW w_recent AS (PARTITION BY product_id ORDER BY cumulative_quantity_rounded_2, id);
