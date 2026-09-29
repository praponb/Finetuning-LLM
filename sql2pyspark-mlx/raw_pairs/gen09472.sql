WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE unit_price <= 190
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY product_id, order_id ORDER BY line_total, id) AS line_total_bucket,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share,
        LEAD(line_total, 1, 0) OVER (ORDER BY unit_price, id) AS next_line_total,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY line_total, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS unit_price_avg_5_prior
    FROM cleaned
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY line_total_bucket DESC NULLS FIRST, id) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.line_total,
        e.next_line_total,
        o.amount,
        o.channel,
        o.discount
    FROM ranked e
    JOIN orders o
        ON e.order_id = o.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount) OVER (ORDER BY line_total, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY line_total, id) AS event_seq
    FROM enriched
),
ranked_order_items AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC NULLS LAST, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY channel ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6,
        NTILE(10) OVER w_part AS event_seq_bucket,
        NTILE(4) OVER w_ordered AS cumulative_discount_bucket,
        LAST_VALUE(cumulative_discount, TRUE) OVER (PARTITION BY channel ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_discount,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY line_total) AS line_total_percent_rank
    FROM ranked_order_items
    WINDOW w_part AS (ORDER BY event_seq, id), w_ordered AS (PARTITION BY channel ORDER BY cumulative_discount, id)
)
-- Join a dimension and aggregate
SELECT
    p.status,
    p.channel AS order_channel,
    COUNT(*) AS rows_total,
    MAX(s.event_seq) AS max_event_seq
FROM metrics s
JOIN orders p ON s.order_id = p.id
GROUP BY GROUPING SETS ((p.status, p.channel), (p.status))
