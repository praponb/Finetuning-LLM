WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(delivered_date, 'MMM yyyy') AS delivered_date_label
    FROM shipments
    WHERE shipped_date > DATE '2023-04-24'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        delivered_date_label,
        NTILE(10) OVER (PARTITION BY MONTH(shipped_date), carrier ORDER BY cost DESC, id ASC) AS cost_bucket,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY status ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status, order_id), 4) AS cost_share,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status, carrier), 4) AS cost_share_2,
        RANK() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS LAST) AS cost_rank
    FROM filtered
    WHERE shipped_date < '2023-07-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id ASC) AS event_seq,
        SUM(cost_share_2) OVER (PARTITION BY DATE_TRUNC('month', shipped_date), carrier ORDER BY cost_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_share_2
    FROM enriched
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(cost_rank) OVER w_ordered AS prev_cost_rank,
        LAG(running_cost_share_2) OVER w_ordered AS prev_running_cost_share_2,
        LEAD(running_cost_share_2) OVER w_ordered AS next_running_cost_share_2,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        SUM(cost_bucket) OVER (ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_bucket
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY carrier ORDER BY cost_rank ASC, id ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date_label,
        cost_bucket,
        cost_share_2,
        cost_rank,
        event_seq,
        next_running_cost_share_2,
        cumulative_cost_bucket,
        MIN(cost_share) OVER (PARTITION BY carrier ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_cost_share,
        MAX(cost_share) OVER (PARTITION BY carrier ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost_share,
        AVG(cost_share) OVER (PARTITION BY carrier ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost_share,
        SUM(cost_share) OVER (PARTITION BY carrier ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost_share
    FROM metrics_shipments
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', shipped_date) AS activity_month,
        carrier,
        COUNT(DISTINCT carrier) AS distinct_carriers,
        ROUND(AVG(running_avg_cost_share), 2) AS avg_running_avg_cost_share,
        ROUND(AVG(running_max_cost_share), 2) AS avg_running_max_cost_share,
        COUNT(*) FILTER (WHERE carrier = 'DHL') AS dhl_rows,
        COUNT(*) AS cnt,
        SUM(next_running_cost_share_2) FILTER (WHERE carrier = 'FedEx') AS fedex_total_next_running_cost_share_2
    FROM with_windows
    GROUP BY 1, 2
)
SELECT
    activity_month,
    carrier,
    distinct_carriers,
    avg_running_avg_cost_share
FROM grouped
ORDER BY carrier ASC NULLS FIRST;
