WITH with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY supplier_id ORDER BY stock_qty ASC, id ASC) AS stock_qty_bucket,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), category ORDER BY price DESC, id ASC) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY stock_qty DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date ASC, id ASC) AS event_seq_2
    FROM products
),
remaining_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT
    SELECT id
    FROM products
    WHERE stock_qty BETWEEN 271 AND 326
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    SELECT
        a.id,
        a.product_name,
        a.category,
        a.cost,
        a.stock_qty,
        a.launch_date,
        a.tags,
        a.supplier_id,
        a.seq_no,
        s.city,
        s.country
    FROM verified a
    INNER JOIN suppliers s
        ON a.supplier_id = s.id
),
with_dims AS (
    SELECT
        src.*,
        b.supplier_name,
        b.country AS supplier_country,
        b.rating
    FROM enriched src
    LEFT JOIN suppliers b
        ON src.supplier_id <=> b.id
),
enriched_products AS (
    SELECT src.*, d.supplier_name AS supplier_supplier_name
    FROM with_dims src
    LEFT JOIN suppliers d
        ON src.supplier_id = d.id
    WHERE d.city IS NULL
)
SELECT *
FROM (
    SELECT
        country,
        supplier_country,
        stock_qty
    FROM enriched_products
) src
PIVOT (
    MAX(stock_qty) AS value_sum
    FOR supplier_country IN ('SG', 'US')
)
