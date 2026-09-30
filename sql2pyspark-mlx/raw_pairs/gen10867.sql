WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(monthly_fee, TRUE) OVER (PARTITION BY status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_monthly_fee,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share_2,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM valid
    WINDOW w_recent AS (ORDER BY end_date DESC, id)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_int,
        last_known_monthly_fee,
        monthly_fee_share_2,
        event_seq,
        ROUND(monthly_fee_int * 1.15, 2) AS monthly_fee_int_scaled
    FROM with_windows
    WHERE start_date >= TO_DATE('2023-05-07')
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY last_known_monthly_fee DESC NULLS LAST, id) AS latest_rank
        FROM filtered
    ) ranked
    WHERE latest_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(status, 1, 4) AS status_prefix
    FROM latest
    WHERE end_date >= DATE '2023-07-10'
      AND status = 'PAUSED'
)
-- Multi-level aggregation
SELECT
    status,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    APPROX_PERCENTILE(last_known_monthly_fee, ARRAY(0.25, 0.5, 0.75)) AS last_known_monthly_fee_quartiles,
    COUNT(DISTINCT status) AS distinct_status
FROM cleaned
GROUP BY status
HAVING COUNT(*) > 1;
