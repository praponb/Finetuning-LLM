WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        QUARTER(last_restock) AS last_restock_quarter
    FROM inventory
    WHERE last_restock >= DATE '2023-07-23'
),
joined AS (
    SELECT
        a.id,
        a.store_id,
        d.region,
        d.sqft,
        d.store_name
    FROM valid a
    JOIN stores d
        ON a.store_id = d.id
),
deduped AS (
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, region ORDER BY sqft, id) = 1
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY sqft NULLS FIRST) AS sqft_percent_rank,
        SUM(sqft) OVER (PARTITION BY store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
        LAST_VALUE(region) IGNORE NULLS OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_region,
        LAG(sqft, 1) OVER (PARTITION BY store_id ORDER BY sqft, id) AS prev_sqft,
        NTILE(3) OVER (ORDER BY sqft DESC, id) AS sqft_bucket
    FROM deduped
    WHERE store_id IN (1, 6)
)
SELECT
    id,
    region,
    sqft,
    store_name,
    running_sqft,
    last_known_region,
    prev_sqft,
    sqft_bucket,
    CASE WHEN store_name RLIKE '^(Old Town|Downtown Express)$' THEN 'GROUP_A' WHEN store_name RLIKE '^(Siam Flagship|Airport Kiosk)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS store_name_zone
FROM with_windows
ORDER BY sqft NULLS LAST;
