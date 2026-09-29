WITH cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        REGEXP_REPLACE(store_name, ' ', '') AS store_name_slug,
        COALESCE(region, 'NONE') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
with_last AS (
    SELECT s.*, x.inventory_quantity
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    SELECT
        *,
        SUM(inventory_quantity) OVER w_recent AS last7_sum_inventory_quantity,
        MIN(inventory_quantity) OVER w_recent AS last7_min_inventory_quantity,
        COUNT(inventory_quantity) OVER w_recent AS last7_count_inventory_quantity,
        AVG(inventory_quantity) OVER w_recent AS last7_avg_inventory_quantity,
        ROW_NUMBER() OVER (ORDER BY inventory_quantity DESC, id) AS seq_no
    FROM with_last
    WINDOW w_recent AS (PARTITION BY region, city ORDER BY inventory_quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored_stores AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        inventory_quantity,
        seq_no,
        MAX(last7_sum_inventory_quantity) OVER w_recent AS running_max_last7_sum_inventory_quantity,
        SUM(last7_sum_inventory_quantity) OVER w_recent AS running_sum_last7_sum_inventory_quantity,
        MIN(last7_sum_inventory_quantity) OVER w_recent AS running_min_last7_sum_inventory_quantity
    FROM scored
    WHERE region IN ('North', 'West', 'Central')
    WINDOW w_recent AS (PARTITION BY city, region ORDER BY last7_avg_inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        LEAD(running_sum_last7_sum_inventory_quantity) OVER w_running AS next_running_sum_last7_sum_inventory_quantity,
        LAG(running_sum_last7_sum_inventory_quantity) OVER w_running AS prev_running_sum_last7_sum_inventory_quantity,
        LEAD(running_max_last7_sum_inventory_quantity) OVER w_running AS next_running_max_last7_sum_inventory_quantity,
        ROW_NUMBER() OVER w_running AS row_seq,
        AVG(running_sum_last7_sum_inventory_quantity) OVER (ORDER BY running_max_last7_sum_inventory_quantity, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_sum_last7_sum_inventory_quantity_avg_5_prior
    FROM scored_stores
    WINDOW w_running AS (PARTITION BY city ORDER BY running_sum_last7_sum_inventory_quantity, id)
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.units < 18
        )
)
SELECT
    city,
    BIT_OR(sqft) AS sqft_bit_or,
    COUNT(DISTINCT city) AS distinct_cities,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    SIZE(COLLECT_SET(city)) AS num_cities,
    ROUND(AVG(next_running_max_last7_sum_inventory_quantity), 2) AS avg_next_running_max_last7_sum_inventory_quantity
FROM eligible
GROUP BY 1
ORDER BY avg_next_running_max_last7_sum_inventory_quantity DESC;
