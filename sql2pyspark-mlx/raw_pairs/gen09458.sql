WITH filtered AS (
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        ROUND(quantity * 0.85, 2) AS quantity_scaled
    FROM inventory
    WHERE store_id IN (3, 5)
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS dedup_rank
        FROM filtered
    ) ranked
    WHERE dedup_rank = 1
),
scored AS (
    SELECT
        *,
        NTILE(3) OVER (ORDER BY quantity_scaled, id) AS quantity_scaled_bucket,
        SUM(quantity_scaled) OVER (PARTITION BY store_id ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_scaled,
        SUM(quantity_scaled) OVER (PARTITION BY product_id ORDER BY quantity_scaled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_quantity_scaled_2,
        LEAD(quantity_scaled, 1) OVER (PARTITION BY store_id ORDER BY dedup_rank, id) AS next_quantity_scaled
    FROM ranked
    WHERE product_id = 6
),
cleaned AS (
    SELECT
        id,
        store_id,
        last_restock,
        quantity_scaled,
        dedup_rank,
        quantity_scaled_bucket,
        running_quantity_scaled,
        running_quantity_scaled_2,
        next_quantity_scaled,
        ROUND(next_quantity_scaled, 0) AS next_quantity_scaled_rounded
    FROM scored
    WHERE running_quantity_scaled_2 = 169
),
ranked_inventory AS (
    SELECT
        id,
        store_id,
        quantity_scaled,
        dedup_rank,
        running_quantity_scaled,
        running_quantity_scaled_2,
        next_quantity_scaled_rounded
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY quantity_scaled DESC NULLS LAST) AS row_num
        FROM cleaned
    ) t
    WHERE row_num = 1
),
scored_inventory AS (
    SELECT
        id,
        running_quantity_scaled,
        next_quantity_scaled_rounded,
        ROW_NUMBER() OVER w_hist AS event_seq,
        NTILE(10) OVER w_ordered AS next_quantity_scaled_rounded_bucket,
        PERCENT_RANK() OVER w_ordered_2 AS next_quantity_scaled_rounded_percent_rank,
        AVG(next_quantity_scaled_rounded) OVER (PARTITION BY store_id ORDER BY dedup_rank, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_quantity_scaled_rounded_avg_5,
        AVG(next_quantity_scaled_rounded) OVER (ORDER BY next_quantity_scaled_rounded, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS next_quantity_scaled_rounded_avg_6
    FROM ranked_inventory
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY dedup_rank, id), w_ordered AS (ORDER BY next_quantity_scaled_rounded, id), w_ordered_2 AS (PARTITION BY store_id ORDER BY next_quantity_scaled_rounded NULLS FIRST)
)
SELECT
    CASE WHEN running_quantity_scaled >= 306 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_quantity_scaled_tier,
    PERCENTILE_APPROX(next_quantity_scaled_rounded_avg_6, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_scaled_rounded_avg_6_quartiles,
    PERCENTILE_APPROX(next_quantity_scaled_rounded_avg_6, ARRAY(0.25, 0.5, 0.75)) AS next_quantity_scaled_rounded_avg_6_quartiles_2,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored_inventory
GROUP BY CASE WHEN running_quantity_scaled >= 306 THEN 'PREMIUM' ELSE 'STANDARD' END;
