WITH cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        CONCAT(status, '/', carrier) AS status_carrier,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE delivered_date > DATE '2023-07-23'
      AND carrier IS NOT DISTINCT FROM 'UPS'
),
base AS (
    SELECT
        id,
        order_id,
        carrier,
        cost,
        status,
        status_carrier,
        CAST(cost AS INT) AS cost_int
    FROM cleaned
    WHERE shipped_date >= DATE '2023-02-21'
),
metrics AS (
    SELECT
        id,
        carrier,
        status,
        status_carrier,
        RANK() OVER (ORDER BY cost_int NULLS LAST) AS cost_int_rank,
        SUM(cost_int) OVER (PARTITION BY carrier ORDER BY cost_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_int,
        LAG(cost_int, 1) OVER (ORDER BY cost_int, id) AS prev_cost_int
    FROM base
),
with_windows AS (
    SELECT
        *,
        LEAD(prev_cost_int) OVER w_hist AS next_prev_cost_int,
        LAG(prev_cost_int) OVER w_hist AS prev_prev_cost_int,
        LEAD(cost_int_rank) OVER w_hist AS next_cost_int_rank,
        LAG(cost_int_rank) OVER w_hist AS prev_cost_int_rank,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM metrics
    WINDOW w_hist AS (PARTITION BY carrier ORDER BY cumulative_cost_int, id)
),
enriched AS (
    SELECT
        *,
        FIRST_VALUE(next_prev_cost_int, TRUE) OVER (PARTITION BY carrier ORDER BY next_cost_int_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_prev_cost_int,
        ROW_NUMBER() OVER (ORDER BY event_seq DESC, id) AS row_seq,
        AVG(cumulative_cost_int) OVER (PARTITION BY carrier ORDER BY cost_int_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_cost_int_avg_6
    FROM with_windows
)
SELECT
    CASE WHEN cumulative_cost_int >= 305 THEN 'HIGH' ELSE 'LOW' END AS cumulative_cost_int_tier,
    carrier,
    CASE WHEN cumulative_cost_int_avg_6 >= 380 THEN 'HIGH' ELSE 'LOW' END AS cumulative_cost_int_avg_6_tier,
    COUNT_IF(status = 'DELIVERED') AS delivered_events,
    COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count,
    SUM(row_seq) FILTER (WHERE status = 'DELIVERED') AS delivered_total_row_seq
FROM enriched
GROUP BY cumulative_cost_int_tier, carrier, cumulative_cost_int_avg_6_tier
ORDER BY in_transit_count DESC NULLS FIRST
