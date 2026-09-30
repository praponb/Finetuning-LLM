WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', shipped_date), order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY cost ASC, id ASC) AS cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC) AS row_seq_2
    FROM shipments
    WHERE carrier IN ('FedEx', 'UPS')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost ASC, id ASC) AS rn
    FROM scored
    QUALIFY rn = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        first_known_carrier,
        cost_bucket,
        row_seq,
        row_seq_2,
        rn,
        LOWER(carrier) AS carrier_lower,
        ROUND(row_seq_2, 2) AS row_seq_2_rounded,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM latest
    WHERE carrier IS DISTINCT FROM 'Kerry'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        cost_bucket,
        row_seq,
        row_seq_2,
        rn,
        carrier_clean,
        CUME_DIST() OVER w_part AS row_seq_2_cume_dist,
        AVG(row_seq_2) OVER (PARTITION BY status ORDER BY row_seq ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS row_seq_2_avg_5,
        ROW_NUMBER() OVER w_hist AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id ASC) AS event_seq_2
    FROM filtered
    WINDOW w_part AS (PARTITION BY DATE_TRUNC('MONTH', shipped_date) ORDER BY row_seq_2 DESC NULLS LAST), w_hist AS (PARTITION BY status ORDER BY delivered_date ASC, id ASC)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_seq_2_avg_5 DESC, id ASC) AS latest_rank
        FROM with_windows
    ) x
    WHERE latest_rank = 1
),
with_windows_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        cost,
        row_seq,
        row_seq_2,
        rn,
        row_seq_2_avg_5,
        AVG(row_seq_2_cume_dist) OVER w_hist AS running_avg_row_seq_2_cume_dist,
        SUM(row_seq_2_cume_dist) OVER w_hist AS running_sum_row_seq_2_cume_dist,
        MAX(row_seq_2_cume_dist) OVER w_hist AS running_max_row_seq_2_cume_dist,
        ROW_NUMBER() OVER w_ordered AS event_seq_3
    FROM deduped
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY row_seq_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY order_id ORDER BY row_seq_2_avg_5 DESC, id ASC)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        cost,
        row_seq_2,
        rn,
        row_seq_2_avg_5,
        running_avg_row_seq_2_cume_dist,
        event_seq_3,
        NTILE(10) OVER (ORDER BY running_max_row_seq_2_cume_dist ASC, id ASC) AS running_max_row_seq_2_cume_dist_bucket,
        NTILE(10) OVER (ORDER BY event_seq_3 ASC, id ASC) AS event_seq_3_bucket,
        FIRST_VALUE(row_seq_2) IGNORE NULLS OVER (ORDER BY running_avg_row_seq_2_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_seq_2,
        LAG(row_seq_2_avg_5, 1) OVER (ORDER BY row_seq ASC, id ASC) AS prev_row_seq_2_avg_5,
        AVG(cost) OVER (ORDER BY event_seq_3 ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2
    FROM with_windows_shipments
)
SELECT
    id,
    row_seq_2,
    rn,
    event_seq_3,
    running_max_row_seq_2_cume_dist_bucket,
    event_seq_3_bucket,
    first_known_row_seq_2,
    prev_row_seq_2_avg_5,
    cost_avg_2
FROM metrics
ORDER BY id DESC NULLS FIRST
