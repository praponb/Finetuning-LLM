WITH joined AS (
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        p.price,
        p.cost
    FROM inventory s
    LEFT JOIN products p
        ON s.product_id = p.id
    WHERE p.supplier_id IS NULL
),
enriched AS (
    SELECT
        id,
        cost,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY price NULLS FIRST) AS price_percent_rank,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY cost DESC NULLS LAST) AS cost_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY cost DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY cost, id) AS row_seq
    FROM joined
),
scored AS (
    SELECT
        *,
        LAST_VALUE(cost_dense_rank, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost_dense_rank,
        LEAD(price_percent_rank, 1) OVER (ORDER BY row_seq, id) AS next_price_percent_rank,
        SUM(cost) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
    FROM enriched
),
filtered AS (
    SELECT *, row_seq * cost_dense_rank AS row_seq_times_cost_dense_rank
    FROM scored
    WHERE cost >= 220
),
filtered_inventory AS (
    SELECT *, ROUND(row_seq_times_cost_dense_rank * 0.85, 2) AS adjusted_row_seq_times_cost_dense_rank
    FROM filtered
    WHERE last_known_cost_dense_rank >= 4
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(row_seq) IGNORE NULLS OVER w_part AS last_known_row_seq,
        ROW_NUMBER() OVER (ORDER BY adjusted_row_seq_times_cost_dense_rank, id) AS event_seq_2,
        LEAD(row_seq_times_cost_dense_rank, 1) OVER (ORDER BY price_percent_rank, id) AS next_row_seq_times_cost_dense_rank
    FROM filtered_inventory
    WINDOW w_part AS (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    SELECT
        CASE WHEN adjusted_row_seq_times_cost_dense_rank >= 89 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_row_seq_times_cost_dense_rank_tier,
        CASE WHEN last_known_cost_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS last_known_cost_dense_rank_tier,
        CASE WHEN cost >= 560 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
        ROUND(AVG(event_seq_2), 2) AS avg_event_seq_2,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    GROUP BY CASE WHEN adjusted_row_seq_times_cost_dense_rank >= 89 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last_known_cost_dense_rank >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN cost >= 560 THEN 'HIGH' ELSE 'LOW' END
    HAVING distinct_ids > 1
)
SELECT
    adjusted_row_seq_times_cost_dense_rank_tier,
    last_known_cost_dense_rank_tier,
    cost_tier,
    distinct_ids
FROM stats
ORDER BY adjusted_row_seq_times_cost_dense_rank_tier DESC, distinct_ids;
