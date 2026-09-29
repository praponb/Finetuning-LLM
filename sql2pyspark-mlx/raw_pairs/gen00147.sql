WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(TRIM(supplier_name)) AS supplier_name_normalized
    FROM suppliers
    WHERE city IS NOT NULL
      AND country IN ('JP', 'US')
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.supplier_id IN (20, 31)
        )
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.country,
        latest.product_launch_date
    FROM eligible s
    JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY product_launch_date ASC, id ASC) AS row_num
        FROM latest_match
    ) ranked
    WHERE row_num = 1
),
eligible_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price < 430
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible_suppliers s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= s.product_launch_date
        )
)
-- Ranking and gap features
SELECT
    id,
    country,
    ROW_NUMBER() OVER (PARTITION BY YEAR(product_launch_date), country ORDER BY product_launch_date DESC, id ASC) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', product_launch_date), country ORDER BY product_launch_date DESC, id ASC) AS seq_no
FROM not_flagged;
