WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at
    FROM claims
    WHERE adjuster_id <= 7
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        LAG(reported_at, 3) OVER (PARTITION BY policy_id, adjuster_id ORDER BY reported_at DESC, id) AS prev3_reported_at,
        DENSE_RANK() OVER w_all_by_reported_at_ties AS reported_at_dense_rank,
        LAG(claim_date, 2) OVER w_claim_date_year_adjuster_id AS prev2_claim_date,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY reported_at ASC, id))) / 60, 1) AS minutes_since_prev_reported_at,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claim_date DESC, id)) AS days_since_prev_claim_date,
        LEAD(reported_at, 3) OVER w_all_by_reported_at AS next3_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claim_date DESC), 3) AS claim_date_percent_rank,
        FIRST_VALUE(claim_date) OVER (PARTITION BY policy_id, adjuster_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date) ORDER BY reported_at), 3) AS reported_at_percent_rank,
        LEAD(claim_date) OVER w_policy_id_adjuster_id_by_rep AS next_claim_date,
        LAST_VALUE(claim_date) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at) ORDER BY claim_date), 3) AS claim_date_cume_dist,
        FIRST_VALUE(reported_at) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        ROW_NUMBER() OVER w_adjuster_id_claim_date_month_2 AS rn,
        DENSE_RANK() OVER w_claim_date_year_adjuster_id_2 AS claim_date_dense_rank,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date
    FROM valid_claims
    WINDOW w_all_by_reported_at_ties AS (ORDER BY reported_at ASC), w_claim_date_year_adjuster_id AS (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claim_date DESC, id), w_all_by_reported_at AS (ORDER BY reported_at ASC, id), w_policy_id_adjuster_id_by_rep AS (PARTITION BY policy_id, adjuster_id ORDER BY reported_at DESC, id), w_adjuster_id_claim_date_month_2 AS (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date) ORDER BY reported_at, id), w_claim_date_year_adjuster_id_2 AS (PARTITION BY YEAR(claim_date), adjuster_id ORDER BY claim_date DESC)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        reported_at_dense_rank,
        prev2_claim_date,
        minutes_since_prev_reported_at,
        days_since_prev_claim_date,
        next3_reported_at,
        claim_date_percent_rank,
        first_claim_date,
        reported_at_percent_rank,
        next_claim_date,
        last_claim_date,
        claim_date_cume_dist,
        first_reported_at,
        rn,
        last_known_claim_date,
        NTILE(3) OVER (ORDER BY last_known_claim_date DESC, id) AS last_known_claim_date_tercile,
        ROUND(CUME_DIST() OVER (ORDER BY prev3_reported_at ASC), 3) AS prev3_reported_at_cume_dist,
        NTILE(10) OVER (ORDER BY prev3_reported_at ASC, id) AS prev3_reported_at_decile,
        ROUND(claim_date_dense_rank / NULLIF(SUM(claim_date_dense_rank) OVER (PARTITION BY YEAR(first_reported_at)), 0), 4) AS claim_date_dense_rank_share_of_first_reported_at,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY YEAR(first_reported_at))) AS distinct_policy_id_in_first_reported_at_year,
        FIRST_VALUE(claim_date_dense_rank) OVER (ORDER BY prev3_reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_dense_rank,
        COUNT(*) OVER (PARTITION BY YEAR(first_reported_at)) AS rows_in_first_reported_at_year,
        RANK() OVER w_all_by_last_known_claim_date AS last_known_claim_date_rank
    FROM with_windows
    WINDOW w_all_by_last_known_claim_date AS (ORDER BY last_known_claim_date DESC, id)
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    reported_at_dense_rank,
    prev2_claim_date,
    minutes_since_prev_reported_at,
    days_since_prev_claim_date,
    next3_reported_at,
    claim_date_percent_rank,
    first_claim_date,
    first_reported_at,
    rn,
    last_known_claim_date,
    last_known_claim_date_tercile,
    prev3_reported_at_decile,
    distinct_policy_id_in_first_reported_at_year,
    rows_in_first_reported_at_year,
    last_known_claim_date_rank,
    ROUND(reported_at_percent_rank * 0.25, 2) AS reported_at_percent_rank_adj
FROM ranked
WHERE rows_in_first_reported_at_year > 4
ORDER BY adjuster_id, id
LIMIT 5
