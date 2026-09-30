WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        DATE_FORMAT(delivered_date, 'yyyy-MM-dd') AS delivered_date_label,
        DATEDIFF(delivered_date, shipped_date) / 7 AS weeks_shipped_date_to_delivered_date,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-05-27')
      AND carrier IS DISTINCT FROM 'FedEx'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(weeks_shipped_date_to_delivered_date) OVER w_part AS running_sum_weeks_shipped_date_to_delivered_date,
        AVG(weeks_shipped_date_to_delivered_date) OVER w_part AS running_avg_weeks_shipped_date_to_delivered_date,
        MAX(weeks_shipped_date_to_delivered_date) OVER w_part AS running_max_weeks_shipped_date_to_delivered_date,
        LAST_VALUE(weeks_shipped_date_to_delivered_date, TRUE) OVER w_ordered AS last_known_weeks_shipped_date_to_delivered_date
    FROM cleaned
    WHERE carrier IN ('Flash', 'Kerry')
    WINDOW w_part AS (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY carrier ORDER BY weeks_shipped_date_to_delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost NULLS FIRST, id) AS latest_rank
        FROM metrics
    ) x
    WHERE latest_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(weeks_shipped_date_to_delivered_date) OVER w_ordered AS running_min_weeks_shipped_date_to_delivered_date,
        AVG(weeks_shipped_date_to_delivered_date) OVER w_ordered AS running_avg_weeks_shipped_date_to_delivered_date_2,
        SUM(weeks_shipped_date_to_delivered_date) OVER w_ordered AS running_sum_weeks_shipped_date_to_delivered_date_2,
        MAX(weeks_shipped_date_to_delivered_date) OVER w_ordered AS running_max_weeks_shipped_date_to_delivered_date_2
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY carrier ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        cost,
        delivered_date_label,
        weeks_shipped_date_to_delivered_date,
        carrier_clean,
        running_avg_weeks_shipped_date_to_delivered_date,
        last_known_weeks_shipped_date_to_delivered_date,
        running_min_weeks_shipped_date_to_delivered_date,
        running_avg_weeks_shipped_date_to_delivered_date_2,
        running_sum_weeks_shipped_date_to_delivered_date_2,
        LEAD(last_known_weeks_shipped_date_to_delivered_date) OVER (PARTITION BY order_id ORDER BY running_sum_weeks_shipped_date_to_delivered_date, id) AS next_last_known_weeks_shipped_date_to_delivered_date,
        LAG(last_known_weeks_shipped_date_to_delivered_date) OVER (PARTITION BY order_id ORDER BY running_sum_weeks_shipped_date_to_delivered_date, id) AS prev_last_known_weeks_shipped_date_to_delivered_date,
        LEAD(running_sum_weeks_shipped_date_to_delivered_date) OVER (PARTITION BY order_id ORDER BY running_sum_weeks_shipped_date_to_delivered_date, id) AS next_running_sum_weeks_shipped_date_to_delivered_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY running_sum_weeks_shipped_date_to_delivered_date, id) AS seq_no
    FROM enriched
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.delivered_date_label,
    s.weeks_shipped_date_to_delivered_date,
    s.last_known_weeks_shipped_date_to_delivered_date,
    s.running_min_weeks_shipped_date_to_delivered_date,
    s.running_sum_weeks_shipped_date_to_delivered_date_2,
    s.prev_last_known_weeks_shipped_date_to_delivered_date,
    CASE WHEN carrier RLIKE '^(UPS|Flash)$' THEN 'EXTENDED' WHEN carrier RLIKE '^(DHL|FedEx)$' THEN 'GROUP_B' ELSE 'CORE' END AS carrier_zone,
    (SELECT AVG(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS avg_payment_amount
FROM with_windows s
