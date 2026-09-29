WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        t.coupon_codes,
        pos + 1 AS line_no,
        line.qty
    FROM carts t
    LATERAL VIEW POSEXPLODE(t.items) x AS pos, line
)
-- Ranking and gap features
SELECT
    id,
    line_no,
    qty,
    COUNT(line_no) OVER (PARTITION BY customer_id ORDER BY qty, id, line_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_line_no,
    SUM(line_no) OVER (PARTITION BY customer_id ORDER BY qty, id, line_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_line_no,
    MAX(line_no) OVER (PARTITION BY customer_id ORDER BY qty, id, line_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_line_no,
    COUNT(qty) OVER (PARTITION BY customer_id) AS customer_id_count_qty
FROM exploded;
