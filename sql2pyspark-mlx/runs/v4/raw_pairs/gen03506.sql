WITH latest AS (
    SELECT *
    FROM inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last_restock DESC, id) AS dedup_rank
    FROM latest
    QUALIFY dedup_rank = 1
),
joined AS (
    SELECT s.*, d.store_name
    FROM ranked s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(store_name)) AS store_name_title,
        CAST(dedup_rank AS DOUBLE) AS dedup_rank_str
    FROM joined
    WHERE last_restock > DATE '2023-09-30'
)
SELECT
    s.id,
    s.store_id,
    s.dedup_rank,
    s.store_name,
    s.store_name_title,
    CASE WHEN store_name RLIKE '^(Mall Central|Siam Flagship)$' THEN 'PRIORITY' ELSE 'CORE' END AS store_name_zone,
    (SELECT SUM(x.units) FROM store_sales x WHERE x.store_id = s.store_id) AS sum_store_sale_units
FROM base s
ORDER BY store_id NULLS LAST;
