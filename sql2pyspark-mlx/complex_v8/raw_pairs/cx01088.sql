WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        HOUR(reported_at) AS reported_at_hour
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND policy_id <= 10
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY policy_id ORDER BY claimed_amount) AS claimed_amount_rank,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY reported_at ASC), 3) AS reported_at_percent_rank,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status), 0), 4) AS claimed_amount_share_of_status,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claimed_amount, id) AS claimed_amount_seq,
        ROW_NUMBER() OVER (PARTITION BY reported_at_hour ORDER BY claimed_amount, id) AS seq_no,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY reported_at_hour ORDER BY claimed_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date
    FROM scoped
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        status,
        claimed_amount_rank,
        reported_at_percent_rank,
        claimed_amount_share_of_status,
        claimed_amount_seq,
        reported_at_hour - LAG(reported_at_hour) OVER (PARTITION BY claimed_amount_seq, policy_id ORDER BY reported_at DESC, id) AS reported_at_hour_change,
        RANK() OVER w_claimed_amount_seq_policy_id_2 AS reported_at_rank,
        COUNT(*) OVER (PARTITION BY TO_DATE(reported_at)) AS rows_in_reported_at_to_date,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY TO_DATE(reported_at))) AS distinct_claim_type_in_reported_at_to_date,
        MIN(claimed_amount_avg_4) OVER (PARTITION BY TO_DATE(reported_at)) AS reported_at_to_date_min_claimed_amount_avg_4,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY claimed_amount_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date_2,
        ROUND(CUME_DIST() OVER w_all_by_claimed_amount_seq_ti, 3) AS claimed_amount_seq_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY claimed_amount_seq, policy_id ORDER BY reported_at DESC), 3) AS reported_at_cume_dist
    FROM with_trends
    WINDOW w_claimed_amount_seq_policy_id_2 AS (PARTITION BY claimed_amount_seq, policy_id ORDER BY reported_at DESC), w_all_by_claimed_amount_seq_ti AS (ORDER BY claimed_amount_seq DESC)
),
totals AS (
    -- Group-level totals
    SELECT
        status,
        distinct_claim_type_in_reported_at_to_date,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        SUM(claimed_amount_seq) FILTER (WHERE status = 'REJECTED') AS claimed_amount_seq_status_rejected
    FROM with_windows
    WHERE YEAR(last_known_claim_date_2) = 2025
    GROUP BY 1, 2
    HAVING AVG(claimed_amount_seq_cume_dist) > 0.4
),
with_trends_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY claimed_amount_seq_status_rejected) AS claimed_amount_seq_status_rejected_rank,
        NTILE(4) OVER w_all_by_claimed_amount_seq_st_2 AS claimed_amount_seq_status_rejected_quartile,
        ROW_NUMBER() OVER (ORDER BY claimed_amount_seq_status_rejected, status, distinct_claim_type_in_reported_at_to_date) AS claimed_amount_seq_status_rejected_seq
    FROM totals
    WINDOW w_all_by_claimed_amount_seq_st_2 AS (ORDER BY claimed_amount_seq_status_rejected, status, distinct_claim_type_in_reported_at_to_date)
)
-- Final projection
SELECT
    status,
    distinct_claim_type_in_reported_at_to_date,
    status_list,
    claimed_amount_seq_status_rejected,
    claimed_amount_seq_status_rejected_rank,
    claimed_amount_seq_status_rejected_seq,
    LENGTH(TRIM(status)) AS status_len
FROM with_trends_2
WHERE distinct_claim_type_in_reported_at_to_date > 5
ORDER BY claimed_amount_seq_status_rejected_seq
