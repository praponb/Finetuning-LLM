WITH scored AS (
    SELECT
        *,
        LAST_VALUE(cost) IGNORE NULLS OVER w_hist AS last_known_cost,
        ROW_NUMBER() OVER w_running AS seq_no,
        ROW_NUMBER() OVER w_part AS event_seq,
        NTILE(5) OVER (ORDER BY price DESC, id ASC) AS price_bucket,
        NTILE(4) OVER w_part AS cost_bucket
    FROM products
    WINDOW w_hist AS (PARTITION BY supplier_id ORDER BY launch_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY launch_date DESC, id ASC), w_part AS (ORDER BY cost ASC, id ASC)
)
SELECT
    id,
    brand,
    price,
    cost,
    stock_qty,
    supplier_id,
    last_known_cost,
    seq_no,
    event_seq
FROM scored
ORDER BY stock_qty ASC NULLS LAST, last_known_cost DESC
