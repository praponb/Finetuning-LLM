WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY cost DESC, id) AS cost_bucket,
        MIN(cost) OVER (PARTITION BY carrier, order_id) AS carrier_min_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        AVG(cost) OVER (PARTITION BY order_id, carrier ORDER BY delivered_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3
    FROM shipments
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        cost,
        cost_bucket,
        carrier_min_cost,
        cost_share,
        ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY carrier_min_cost DESC, id) AS event_seq,
        NTILE(3) OVER (PARTITION BY carrier ORDER BY cost_share DESC, id) AS cost_share_bucket,
        LEAD(cost_bucket, 2) OVER (ORDER BY shipped_date, id) AS next_cost_bucket,
        SUM(cost_avg_3) OVER (PARTITION BY carrier ORDER BY cost_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_avg_3
    FROM metrics
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        cost_share,
        running_cost_avg_3,
        MAX(carrier_min_cost) OVER w_grp AS running_max_carrier_min_cost,
        SUM(carrier_min_cost) OVER w_grp AS running_sum_carrier_min_cost,
        AVG(carrier_min_cost) OVER w_grp AS running_avg_carrier_min_cost
    FROM scored
    WHERE event_seq > 4
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY running_cost_avg_3, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        cost_share,
        running_cost_avg_3,
        running_max_carrier_min_cost,
        running_avg_carrier_min_cost,
        SUM(running_sum_carrier_min_cost) OVER (ORDER BY cost_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_running_sum_carrier_min_cost,
        MIN(running_sum_carrier_min_cost) OVER (ORDER BY cost_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_running_sum_carrier_min_cost,
        COUNT(running_sum_carrier_min_cost) OVER (ORDER BY cost_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_running_sum_carrier_min_cost,
        AVG(running_sum_carrier_min_cost) OVER (ORDER BY cost_share, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_running_sum_carrier_min_cost
    FROM enriched
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        cost_share,
        running_cost_avg_3,
        running_max_carrier_min_cost,
        last7_sum_running_sum_carrier_min_cost,
        last7_min_running_sum_carrier_min_cost,
        last7_avg_running_sum_carrier_min_cost,
        CAST(last7_count_running_sum_carrier_min_cost AS STRING) AS last7_count_running_sum_carrier_min_cost_num,
        CASE WHEN running_cost_avg_3 >= 538 THEN 'High' WHEN running_cost_avg_3 >= 69 THEN 'Medium' ELSE 'Low' END AS running_cost_avg_3_band
    FROM with_windows
    WHERE running_cost_avg_3 >= 188
)
SELECT
    id,
    running_cost_avg_3,
    running_max_carrier_min_cost,
    last7_sum_running_sum_carrier_min_cost,
    last7_min_running_sum_carrier_min_cost,
    last7_avg_running_sum_carrier_min_cost
FROM cleaned
ORDER BY id DESC NULLS LAST
