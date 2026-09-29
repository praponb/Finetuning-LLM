WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT id, supplier_name
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS row_num
        FROM suppliers
    ) t
    WHERE row_num = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IS NOT NULL
        )
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.launch_date
    FROM eligible s
    JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY launch_date, id) seq_no,
        ROW_NUMBER() OVER w_hist event_seq,
        ROW_NUMBER() OVER w_hist seq_no_2,
        ROW_NUMBER() OVER w_hist row_seq,
        ROW_NUMBER() OVER w_hist event_seq_2
    FROM with_latest
    WINDOW w_hist AS (ORDER BY launch_date DESC, id)
),
eligible_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date <= s.launch_date
        )
)
SELECT
    id,
    supplier_name,
    launch_date,
    seq_no_2,
    row_seq,
    event_seq_2
FROM eligible_suppliers
ORDER BY launch_date DESC NULLS LAST;
