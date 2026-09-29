WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        COALESCE(amount, 0) AS amount_filled
    FROM orders
    WHERE discount < 0.27
)
-- Join a dimension and aggregate
SELECT
    p.segment,
    s.channel,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.amount), 2) AS total_amount,
    MAX(s.amount_filled) AS max_amount_filled
FROM valid s
INNER JOIN customers p ON s.customer_id = p.id
GROUP BY GROUPING SETS ((p.segment, s.channel), (p.segment))
ORDER BY segment DESC NULLS FIRST;
