WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        status,
        reported_at
    FROM claims
    WHERE claim_type = 'flood'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_reported_at_month_by_reporte AS reported_at_seq,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(reported_at) ORDER BY reported_at, id), 3) AS reported_at_percent_rank,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY reported_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY YEAR(reported_at) ORDER BY reported_at DESC, id))) / 60, 1) AS minutes_since_prev_reported_at
    FROM prepared
    WINDOW w_reported_at_month_by_reporte AS (PARTITION BY MONTH(reported_at) ORDER BY reported_at, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(reported_at_seq, 2) OVER (ORDER BY second_reported_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_seq,
        ROUND(reported_at_percent_rank - AVG(reported_at_percent_rank) OVER (PARTITION BY status, reported_at_seq), 2) AS reported_at_percent_rank_vs_status_reported_at_s,
        ROUND(reported_at_percent_rank / NULLIF(SUM(reported_at_percent_rank) OVER (PARTITION BY status, reported_at_seq), 0), 4) AS reported_at_percent_rank_share_of_status_reporte,
        ROW_NUMBER() OVER (ORDER BY second_reported_at NULLS LAST, id) AS seq_no
    FROM metrics
)
-- Subtotals with ROLLUP
SELECT
    reported_at_seq,
    status,
    GROUPING(reported_at_seq) AS is_total_reported_at_seq,
    MIN(reported_at) AS first_reported_at
FROM with_windows
GROUP BY ROLLUP(reported_at_seq, status)
ORDER BY reported_at_seq NULLS LAST, status NULLS LAST
