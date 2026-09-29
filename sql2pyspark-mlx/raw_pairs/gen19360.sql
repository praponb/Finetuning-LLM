WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN quantity >= 242 THEN 'High' WHEN quantity >= 58 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock >= DATE '2023-08-16'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(quantity) OVER w_grp AS last4_sum_quantity,
        MAX(quantity) OVER w_grp AS last4_max_quantity,
        COUNT(quantity) OVER w_grp AS last4_count_quantity
    FROM valid
    WHERE MONTH(last_restock) IN (6, 8, 10)
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY last_restock, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, d.city
    FROM scored s
    INNER JOIN stores d
        ON s.store_id = d.id
),
joined_inventory AS (
    -- Attach stores attributes
    SELECT e.*, b.store_name
    FROM joined e
    INNER JOIN stores b
        ON e.store_id = b.id
),
scored_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(last4_max_quantity) OVER w_grp AS next_last4_max_quantity,
        LAG(quantity) OVER w_grp AS prev_quantity,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM joined_inventory
    WINDOW w_grp AS (PARTITION BY product_id, city ORDER BY last4_count_quantity, id)
)
-- Join a dimension and aggregate
SELECT
    d.city AS store_city,
    d.region,
    src.city AS inventory_city,
    COUNT(*) AS cnt,
    ROUND(AVG(src.seq_no), 2) AS avg_seq_no
FROM scored_inventory src
INNER JOIN stores d ON src.store_id = d.id
GROUP BY ROLLUP (d.city, d.region, src.city)
