WITH valid AS (
    SELECT
        id,
        store_id,
        last_restock,
        CAST(quantity AS STRING) AS quantity_str,
        DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock
    FROM inventory
    WHERE product_id IN (5, 10)
),
with_dims AS (
    SELECT src.*, s.store_name
    FROM valid src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
metrics AS (
    SELECT
        id,
        store_id,
        last_restock,
        quantity_str,
        days_since_last_restock,
        store_name,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY days_since_last_restock, id) AS days_since_last_restock_bucket,
        LAST_VALUE(days_since_last_restock) IGNORE NULLS OVER (ORDER BY days_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_last_restock,
        MAX(days_since_last_restock) OVER (PARTITION BY store_id) AS store_id_max_days_since_last_restock
    FROM with_dims
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_known_days_since_last_restock DESC NULLS FIRST, id) AS rank_in_group
        FROM metrics
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    SELECT src.*, d.opened_date
    FROM latest src
    JOIN stores d
        ON src.store_id = d.id
),
enriched_inventory AS (
    SELECT
        *,
        AVG(last_known_days_since_last_restock) OVER (PARTITION BY store_id ORDER BY last_known_days_since_last_restock, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last_known_days_since_last_restock_avg_5_prior,
        PERCENT_RANK() OVER w_part AS store_id_max_days_since_last_restock_percent_rank,
        MIN(store_id_max_days_since_last_restock) OVER w_running AS store_id_min_store_id_max_days_since_last_restock,
        NTILE(5) OVER (ORDER BY last_known_days_since_last_restock, id) AS last_known_days_since_last_restock_bucket
    FROM enriched
    WINDOW w_part AS (PARTITION BY store_id ORDER BY store_id_max_days_since_last_restock DESC NULLS FIRST), w_running AS (PARTITION BY store_id)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id_min_store_id_max_days_since_last_restock DESC, id) AS rank_in_group_2
        FROM enriched_inventory
    ) t
    WHERE rank_in_group_2 = 1
),
totals AS (
    SELECT
        DATE_TRUNC('QUARTER', last_restock) AS quarter_start,
        CASE WHEN store_id_max_days_since_last_restock_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END AS store_id_max_days_since_last_restock_percent_rank_tier,
        CASE WHEN days_since_last_restock >= 62 THEN 'LARGE' ELSE 'SMALL' END AS days_since_last_restock_tier,
        COUNT(DISTINCT store_id) AS distinct_store_ids,
        COUNT(DISTINCT store_id) AS distinct_store_ids_2,
        ROUND(AVG(store_id_max_days_since_last_restock), 2) AS avg_store_id_max_days_since_last_restock,
        PERCENTILE_APPROX(store_id_min_store_id_max_days_since_last_restock, ARRAY(0.25, 0.5, 0.75)) AS store_id_min_store_id_max_days_since_last_restock_quartiles,
        APPROX_PERCENTILE(store_id_min_store_id_max_days_since_last_restock, 0.9) AS store_id_min_store_id_max_days_since_last_restock_p90,
        COUNT(*) AS event_count,
        GROUPING_ID() AS grouping_id
    FROM deduped
    GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', last_restock), CASE WHEN store_id_max_days_since_last_restock_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_since_last_restock >= 62 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('QUARTER', last_restock), CASE WHEN store_id_max_days_since_last_restock_percent_rank >= 0.85 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', last_restock)), ())
)
SELECT
    distinct_store_ids,
    distinct_store_ids_2,
    avg_store_id_max_days_since_last_restock,
    store_id_min_store_id_max_days_since_last_restock_quartiles,
    store_id_min_store_id_max_days_since_last_restock_p90,
    event_count
FROM totals
ORDER BY store_id_min_store_id_max_days_since_last_restock_p90 NULLS LAST;
