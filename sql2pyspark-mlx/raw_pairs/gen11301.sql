WITH filtered AS (
    SELECT
        *,
        LENGTH(carrier) carrier_length,
        COALESCE(carrier, 'NONE') carrier_filled
    FROM shipments
    WHERE shipped_date > TO_DATE('2023-03-29')
),
enriched AS (
    SELECT
        src.*,
        o.discount,
        o.order_date,
        o.channel
    FROM filtered src
    JOIN orders o
        ON src.order_id = o.id
),
with_windows AS (
    SELECT
        id,
        carrier,
        shipped_date,
        carrier_filled,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY MONTH(order_date), channel ORDER BY delivered_date ASC, id ASC) row_seq,
        AVG(discount) OVER (PARTITION BY channel, order_id ORDER BY cost ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) discount_avg_2_prior,
        LAG(cost, 1, 0) OVER (PARTITION BY DATE_TRUNC('week', delivered_date) ORDER BY discount ASC, id ASC) prev_cost,
        LAST_VALUE(carrier_length, TRUE) OVER (PARTITION BY YEAR(order_date), order_id ORDER BY carrier_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_carrier_length,
        SUM(carrier_length) OVER (PARTITION BY status ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_carrier_length
    FROM enriched
),
cleaned AS (
    SELECT *, CEIL(row_seq) row_seq_whole
    FROM with_windows
    WHERE carrier IS NOT NULL
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(row_seq) IGNORE NULLS OVER w_recent first_known_row_seq,
        AVG(prev_cost) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) prev_cost_avg_6_prior
    FROM cleaned
    WINDOW w_recent AS (ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_shipments AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY row_seq_whole DESC, id ASC) row_seq_2,
        COUNT(row_seq) OVER w_part order_date_period_count_row_seq,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id ASC) seq_no,
        MAX(running_carrier_length) OVER (PARTITION BY carrier) carrier_max_running_carrier_length,
        LEAD(discount_avg_2_prior, 1) OVER w_running next_discount_avg_2_prior
    FROM metrics
    WINDOW w_part AS (PARTITION BY DATE_TRUNC('month', order_date), carrier), w_running AS (PARTITION BY DATE_TRUNC('YEAR', shipped_date), carrier ORDER BY prev_cost_avg_6_prior ASC, id ASC)
)
SELECT
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    SUM(row_seq_2) FILTER (WHERE carrier = 'Flash') AS flash_total_row_seq_2,
    SIZE(COLLECT_SET(carrier)) AS num_carriers
FROM with_windows_shipments
GROUP BY CUBE (DATE_TRUNC('MONTH', shipped_date), DATE_TRUNC('WEEK', shipped_date));
