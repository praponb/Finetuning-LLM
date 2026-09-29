WITH valid AS (
    SELECT *, COALESCE(quantity, 0) AS quantity_filled
    FROM inventory
    WHERE quantity <= 76
),
base AS (
    SELECT
        id,
        store_id,
        last_restock,
        DATE_ADD(last_restock, 2) AS last_restock_plus_2d,
        COALESCE(quantity, 0) AS quantity_filled_2
    FROM valid
    WHERE last_restock >= DATE '2023-07-09'
),
joined AS (
    SELECT
        s.id,
        s.store_id,
        s.last_restock_plus_2d,
        s.quantity_filled_2,
        d.opened_date
    FROM base s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
enriched AS (
    SELECT
        src.id,
        src.store_id,
        src.opened_date,
        p.city,
        p.region
    FROM joined src
    LEFT JOIN stores p
        ON src.store_id = p.id
),
joined_inventory AS (
    SELECT
        a.id,
        a.store_id,
        a.region,
        d.opened_date AS store_opened_date,
        d.region AS store_region
    FROM enriched a
    JOIN stores d
        ON a.store_id = d.id
)
SELECT
    s.id,
    s.store_id,
    s.region,
    s.store_opened_date,
    CASE WHEN region RLIKE '^(East|Central)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS region_zone,
    (SELECT SUM(x.amount) FROM orders x WHERE x.store_id = s.store_id) AS sum_order_amount
FROM joined_inventory s
