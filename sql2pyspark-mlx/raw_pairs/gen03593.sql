WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(quantity) OVER w_ordered AS prev_quantity,
        LEAD(quantity) OVER w_ordered AS next_quantity,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM inventory
    WHERE store_id IN (4, 5)
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY quantity, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        next_quantity,
        seq_no,
        DATE_ADD(last_restock, 42) AS last_restock_plus_42d
    FROM scored
    WHERE prev_quantity >= 102
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.region,
        b.store_name
    FROM filtered e
    JOIN stores b
        ON e.store_id = b.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id, region ORDER BY quantity NULLS FIRST) AS rn
        FROM with_dims
    ) x
    WHERE rn = 1
)
SELECT
    s.id,
    s.product_id,
    s.quantity,
    s.last_restock,
    s.next_quantity,
    s.seq_no,
    s.region,
    s.store_name,
    s.rn,
    CASE WHEN region RLIKE '^(West|North)$' THEN 'OTHER_ZONE' WHEN region RLIKE '^(South|East)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS region_zone,
    (SELECT MIN(s2.units) FROM store_sales s2 WHERE s2.product_id = s.product_id) AS min_store_sale_units
FROM latest s
