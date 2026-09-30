WITH latest AS (
    SELECT
        id,
        order_id,
        quantity
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity NULLS FIRST, id) = 1
),
deduped AS (
    SELECT id, quantity
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS latest_rank
        FROM latest
    ) ranked
    WHERE latest_rank = 1
),
with_windows AS (
    SELECT
        *,
        SUM(quantity) OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity,
        LAST_VALUE(quantity) IGNORE NULLS OVER (ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_quantity,
        DENSE_RANK() OVER (ORDER BY quantity NULLS FIRST) quantity_dense_rank
    FROM deduped
),
metrics AS (
    SELECT
        id,
        quantity,
        running_quantity,
        last_known_quantity,
        SUM(quantity_dense_rank) OVER (ORDER BY quantity_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity_dense_rank,
        SUM(quantity_dense_rank) OVER (ORDER BY running_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity_dense_rank_2,
        SUM(quantity_dense_rank) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_quantity_dense_rank_3
    FROM with_windows
    WHERE quantity_dense_rank != 3
),
scored AS (
    SELECT
        id,
        running_quantity,
        last_known_quantity,
        running_quantity_dense_rank,
        AVG(running_quantity_dense_rank_3) OVER (ORDER BY running_quantity_dense_rank_2, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) running_quantity_dense_rank_3_avg_5_prior,
        AVG(last_known_quantity) OVER w_ordered last_known_quantity_avg_3_prior,
        LAST_VALUE(running_quantity_dense_rank, TRUE) OVER (ORDER BY last_known_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_running_quantity_dense_rank,
        PERCENT_RANK() OVER (ORDER BY running_quantity_dense_rank_3 DESC NULLS LAST) running_quantity_dense_rank_3_percent_rank
    FROM metrics
    WINDOW w_ordered AS (ORDER BY running_quantity_dense_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
)
SELECT
    CASE WHEN running_quantity >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_quantity_tier,
    ROUND(AVG(running_quantity), 2) AS avg_running_quantity,
    PERCENTILE_APPROX(last_known_quantity_avg_3_prior, ARRAY(0.25, 0.5, 0.75)) AS last_known_quantity_avg_3_prior_quartiles
FROM scored
GROUP BY CASE WHEN running_quantity >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING ROUND(AVG(running_quantity), 2) >= 0
ORDER BY avg_running_quantity DESC NULLS FIRST
