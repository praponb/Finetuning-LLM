WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        pos,
        item.product_id,
        item.price,
        item.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) li AS pos, item
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        pos,
        SUM(pos) OVER w_ordered AS running_sum_pos,
        MAX(pos) OVER w_ordered AS running_max_pos,
        AVG(pos) OVER w_ordered AS running_avg_pos,
        MIN(pos) OVER w_ordered AS running_min_pos
    FROM flattened
    WINDOW w_ordered AS (PARTITION BY customer_id, promo ORDER BY price, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        pos,
        running_sum_pos,
        running_max_pos,
        running_avg_pos,
        running_min_pos,
        SUM(pos) OVER w_hist AS running_sum_pos_2,
        AVG(pos) OVER w_hist AS running_avg_pos_2,
        COUNT(pos) OVER w_hist AS running_count_pos,
        ROW_NUMBER() OVER (ORDER BY pos, id, pos) AS event_seq
    FROM metrics
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY running_avg_pos, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(running_max_pos) OVER (ORDER BY running_min_pos, id, pos ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_running_max_pos,
        SUM(running_max_pos) OVER (ORDER BY running_min_pos, id, pos ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_running_max_pos,
        AVG(running_max_pos) OVER (ORDER BY running_min_pos, id, pos ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_running_max_pos
    FROM with_windows
    WHERE event_seq < 5
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_sum_pos_2) OVER (ORDER BY event_seq, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_sum_pos_2,
        SUM(running_sum_pos_2) OVER (ORDER BY event_seq, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_sum_pos_2,
        COUNT(running_sum_pos_2) OVER (ORDER BY event_seq, id, pos ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_sum_pos_2
    FROM metrics_carts
)
SELECT
    id,
    pos,
    running_min_pos,
    running_avg_pos_2,
    running_count_pos,
    event_seq,
    last7_sum_running_max_pos,
    last7_avg_running_max_pos,
    running_sum_running_sum_pos_2
FROM enriched;
