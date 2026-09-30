WITH base AS (
    SELECT *, DATE_FORMAT(last_restock, 'yyyy-MM-dd') AS last_restock_label
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-03-21')
),
enriched AS (
    SELECT
        e.*,
        b.product_name,
        b.brand
    FROM base e
    LEFT JOIN products b
        ON e.product_id = b.id
),
scored AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock_label,
        brand,
        LEAD(quantity) OVER (PARTITION BY brand ORDER BY quantity, id) AS next_quantity,
        LAG(quantity) OVER (PARTITION BY brand ORDER BY quantity, id) AS prev_quantity,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY quantity, id) AS seq_no
    FROM enriched
    WHERE product_id IN (3, 5, 6, 7)
),
joined AS (
    SELECT a.*, b.opened_date
    FROM scored a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    SUM(next_quantity) FILTER (WHERE brand = 'Initech') AS initech_total_next_quantity,
    ROUND(AVG(seq_no), 2) AS avg_seq_no
FROM joined
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', opened_date), CASE WHEN seq_no >= 9 THEN 'HIGH' ELSE 'LOW' END)
HAVING SUM(next_quantity) FILTER (WHERE brand = 'Initech') > 0;
