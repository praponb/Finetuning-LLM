WITH filtered AS (
    SELECT
        *,
        DATE_ADD(last_restock, 66) AS last_restock_plus_66d,
        LAST_DAY(last_restock) AS last_restock_month_end
    FROM inventory
    WHERE last_restock > DATE '2023-07-11'
      AND store_id = 3
),
latest AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock_plus_66d,
        last_restock_month_end
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_month_end, id) AS rank_in_group
        FROM filtered
    ) ranked
    WHERE rank_in_group = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock_plus_66d NULLS FIRST, id) AS rn
    FROM latest
    QUALIFY rn = 1
),
scored AS (
    SELECT
        *,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY last_restock_plus_66d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_quantity,
        FIRST_VALUE(quantity, TRUE) OVER (PARTITION BY YEAR(last_restock_month_end), store_id ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity,
        SUM(quantity) OVER (ORDER BY last_restock_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM ranked
    WHERE last_restock_month_end >= '2025-10-01'
),
enriched AS (
    SELECT
        src.*,
        d.store_name,
        d.region
    FROM scored src
    LEFT JOIN stores d
        ON src.store_id = d.id
)
SELECT
    store_id,
    region,
    SUM(CASE WHEN region = 'South' THEN last_known_quantity ELSE 0 END) AS south_last_known_quantity,
    COUNT(*) AS num_rows,
    ROUND(SUM(first_known_quantity) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) AS first_known_quantity_per_north,
    COUNT(DISTINCT region) AS distinct_regions
FROM enriched
GROUP BY store_id, region
ORDER BY distinct_regions DESC
