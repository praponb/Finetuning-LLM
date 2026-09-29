WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.stock_qty,
        x.price
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty, c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        stock_qty,
        price,
        SUM(price) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        FIRST_VALUE(price, TRUE) OVER (PARTITION BY city ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price
    FROM with_last
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM with_windows
    EXCEPT ALL
    SELECT id, city
    FROM suppliers
    WHERE country = 'UK'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        country,
        rating,
        price,
        first_known_price
    FROM verified
) src
UNPIVOT (
    val FOR measure IN (rating, price, first_known_price)
)
