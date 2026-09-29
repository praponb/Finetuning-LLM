WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total DESC, id) AS rank_in_group
        FROM base
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        unit_price,
        line_total,
        rank_in_group,
        RANK() OVER w_recent AS quantity_rank,
        CORR(quantity, unit_price) OVER w_grp AS corr_quantity_unit_price,
        LAG(quantity, 1) OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id) AS prev_quantity
    FROM latest
    WHERE quantity > 5
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY quantity NULLS LAST), w_grp AS (PARTITION BY order_id, product_id)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.unit_price,
        s.corr_quantity_unit_price,
        s.prev_quantity,
        p.amount
    FROM metrics s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.*,
        b.shipping_fee,
        b.discount
    FROM enriched s
    INNER JOIN orders b
        ON s.order_id = b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(corr_quantity_unit_price) OVER w_recent AS prev_corr_quantity_unit_price,
        LAG(shipping_fee) OVER w_recent AS prev_shipping_fee,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM joined
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY shipping_fee, id)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        corr_quantity_unit_price,
        prev_quantity,
        amount,
        discount,
        prev_corr_quantity_unit_price,
        prev_shipping_fee,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY prev_shipping_fee DESC NULLS LAST, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    order_id,
    CASE WHEN prev_quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END AS prev_quantity_tier,
    APPROX_PERCENTILE(prev_corr_quantity_unit_price, ARRAY(0.25, 0.5, 0.75)) AS prev_corr_quantity_unit_price_quartiles,
    AVG(corr_quantity_unit_price) AS avg_corr_quantity_unit_price,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    SUM(row_seq) AS total_row_seq,
    APPROX_PERCENTILE(prev_shipping_fee, 0.9) AS prev_shipping_fee_p90,
    MIN(prev_quantity) AS min_prev_quantity
FROM ranked
GROUP BY ROLLUP (order_id, CASE WHEN prev_quantity >= 9 THEN 'LARGE' ELSE 'SMALL' END)
HAVING APPROX_PERCENTILE(prev_shipping_fee, 0.9) > 0
ORDER BY min_prev_quantity
