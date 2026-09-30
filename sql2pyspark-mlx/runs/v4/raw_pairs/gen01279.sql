WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        MONTH(delivered_date) AS delivered_date_month,
        COALESCE(carrier, 'UNKNOWN') AS carrier_clean
    FROM shipments
    WHERE order_id IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        delivered_date_month,
        carrier_clean,
        ADD_MONTHS(delivered_date, 12) AS delivered_date_plus_months,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM filtered
    WHERE delivered_date >= TO_DATE('2023-10-19')
      AND carrier IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date_plus_months, id) AS event_seq,
        AVG(delivered_date_month) OVER (PARTITION BY status ORDER BY delivered_date_plus_months, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS delivered_date_month_avg_6
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        delivered_date_month,
        carrier_clean,
        delivered_date_plus_months,
        carrier_filled,
        event_seq,
        delivered_date_month_avg_6,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY status), 4) AS event_seq_share,
        STDDEV_SAMP(delivered_date_month) OVER (PARTITION BY carrier, status) AS carrier_stddev_samp_delivered_date_month
    FROM with_windows
),
enriched_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        delivered_date,
        status,
        delivered_date_month,
        carrier_clean,
        delivered_date_plus_months,
        carrier_filled,
        event_seq,
        delivered_date_month_avg_6,
        event_seq_share,
        carrier_stddev_samp_delivered_date_month,
        LAST_VALUE(event_seq, TRUE) OVER (PARTITION BY status, carrier ORDER BY carrier_stddev_samp_delivered_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq,
        LAG(delivered_date_month, 1) OVER (PARTITION BY carrier ORDER BY event_seq, id) AS prev_delivered_date_month
    FROM enriched
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(event_seq_share) OVER w_running AS running_event_seq_share,
        NTILE(5) OVER (PARTITION BY carrier ORDER BY last_known_event_seq DESC, id) AS last_known_event_seq_bucket,
        AVG(delivered_date_month_avg_6) OVER w_hist AS delivered_date_month_avg_6_avg_3,
        CUME_DIST() OVER (PARTITION BY carrier, status ORDER BY last_known_event_seq DESC NULLS FIRST) AS last_known_event_seq_cume_dist
    FROM enriched_shipments
    WINDOW w_running AS (PARTITION BY status, carrier ORDER BY prev_delivered_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY status ORDER BY last_known_event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY carrier ORDER BY event_seq DESC) = 1
)
SELECT
    id,
    carrier,
    delivered_date,
    status,
    delivered_date_month,
    carrier_clean,
    delivered_date_plus_months,
    carrier_filled,
    event_seq
FROM deduped
