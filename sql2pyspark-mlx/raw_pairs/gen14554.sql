WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) / 365 AS years_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-07-08')
      AND product_id IN (1, 2, 3, 7)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock DESC NULLS FIRST, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity,
        latest_rank,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_quantity,
        MAX(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_quantity,
        MIN(quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_quantity,
        SUM(years_since_last_restock) OVER (PARTITION BY store_id ORDER BY years_since_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_years_since_last_restock
    FROM deduped
    WHERE YEAR(last_restock) >= 2024
),
enriched_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(running_sum_quantity) OVER (PARTITION BY store_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_sum_quantity_avg_2,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY running_sum_quantity, id) AS running_sum_quantity_bucket,
        AVG(running_sum_quantity) OVER (PARTITION BY store_id ORDER BY latest_rank, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_sum_quantity_avg_5_prior
    FROM enriched
)
SELECT
    id,
    store_id,
    quantity,
    running_sum_quantity,
    running_max_quantity,
    running_min_quantity,
    running_years_since_last_restock,
    running_sum_quantity_avg_2,
    running_sum_quantity_bucket
FROM enriched_inventory
ORDER BY running_years_since_last_restock
