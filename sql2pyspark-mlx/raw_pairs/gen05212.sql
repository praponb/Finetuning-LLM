WITH valid AS (
    SELECT
        id,
        store_id,
        last_restock,
        ROUND(quantity * 1.1, 2) AS adjusted_quantity
    FROM inventory
    WHERE quantity > 75
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY last_restock NULLS LAST, id) = 1
),
enriched AS (
    SELECT
        *,
        AVG(adjusted_quantity) OVER (ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS adjusted_quantity_avg_6_prior,
        SUM(adjusted_quantity) OVER (PARTITION BY store_id ORDER BY adjusted_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjusted_quantity
    FROM deduped
),
matched_keys AS (
    SELECT store_id, id
    FROM enriched
    INTERSECT
    SELECT store_id, id
    FROM inventory
    WHERE quantity >= 136
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.store_id = k.store_id AND s.id = k.id
),
metrics AS (
    SELECT
        id,
        store_id,
        last_restock,
        adjusted_quantity,
        running_adjusted_quantity,
        CUME_DIST() OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY adjusted_quantity DESC NULLS FIRST) AS adjusted_quantity_cume_dist,
        MIN(running_adjusted_quantity) OVER (PARTITION BY store_id) AS store_id_min_running_adjusted_quantity,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY running_adjusted_quantity DESC NULLS LAST) AS running_adjusted_quantity_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY YEAR(last_restock) ORDER BY adjusted_quantity_avg_6_prior DESC NULLS FIRST) AS adjusted_quantity_avg_6_prior_percent_rank,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY adjusted_quantity_avg_6_prior DESC NULLS FIRST) AS adjusted_quantity_avg_6_prior_dense_rank
    FROM reconciled
)
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(adjusted_quantity_cume_dist, 0.95) AS adjusted_quantity_cume_dist_p95,
    SUM(adjusted_quantity) AS total_adjusted_quantity,
    PERCENTILE_APPROX(adjusted_quantity_avg_6_prior_dense_rank, 0.5) AS adjusted_quantity_avg_6_prior_dense_rank_p50,
    ROUND(AVG(adjusted_quantity_cume_dist), 2) AS avg_adjusted_quantity_cume_dist
FROM metrics
GROUP BY DATE_TRUNC('MONTH', last_restock)
