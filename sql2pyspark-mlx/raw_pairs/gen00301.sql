WITH filtered AS (
    SELECT *, UPPER(status) AS status_upper
    FROM shipments
    WHERE delivered_date >= DATE '2023-04-24'
      AND order_id IS NOT NULL
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS LAST, id) = 1
),
enriched AS (
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY MONTH(shipped_date) ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) AS seq_no
    FROM deduped
),
metrics AS (
    SELECT
        *,
        AVG(cumulative_cost) OVER w_grp AS last3_avg_cumulative_cost,
        COUNT(cumulative_cost) OVER w_grp AS last3_count_cumulative_cost,
        MIN(cumulative_cost) OVER w_grp AS last3_min_cumulative_cost
    FROM enriched
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY cumulative_cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY status ORDER BY cost DESC, id) AS cost_bucket,
        AVG(last3_avg_cumulative_cost) OVER (PARTITION BY DATE_TRUNC('week', shipped_date), carrier ORDER BY last3_avg_cumulative_cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last3_avg_cumulative_cost_avg_6,
        ROW_NUMBER() OVER (ORDER BY cost, id) AS seq_no_2
    FROM metrics
),
metrics_shipments AS (
    SELECT
        *,
        LEAD(last3_min_cumulative_cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_last3_min_cumulative_cost,
        LAG(last3_min_cumulative_cost) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_last3_min_cumulative_cost,
        LAG(last3_avg_cumulative_cost_avg_6) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_last3_avg_cumulative_cost_avg_6,
        LEAD(last3_avg_cumulative_cost_avg_6) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS next_last3_avg_cumulative_cost_avg_6,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS row_seq
    FROM with_windows
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.status_upper,
    s.cumulative_cost,
    s.seq_no,
    s.last3_avg_cumulative_cost,
    s.last3_count_cumulative_cost,
    CASE WHEN carrier RLIKE '^(Kerry|UPS)$' THEN 'OTHER_ZONE' WHEN carrier RLIKE '^(DHL|FedEx)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS carrier_zone,
    (SELECT AVG(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS avg_shipment_cost
FROM metrics_shipments s
ORDER BY shipped_date;
