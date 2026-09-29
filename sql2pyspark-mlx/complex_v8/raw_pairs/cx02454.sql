WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE policy_id IS NOT NULL
),
with_parent AS (
    -- Attach adjuster attributes
    SELECT s.*, a.team AS adjuster_team
    FROM filtered_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        status_or_unknown,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status_or_unknown), 0), 4) AS claimed_amount_share_of_status_or_unknown,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_1_prior,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status_or_unknown), 2) AS claimed_amount_vs_status_or_unknown_avg,
        MIN(claimed_amount) OVER (PARTITION BY status_or_unknown, adjuster_team) AS status_or_unknown_adjuster_team_min_claimed_amou,
        LEAD(claimed_amount) OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id) AS next_claimed_amount,
        ROUND(CUME_DIST() OVER w_adjuster_id_by_claimed_amoun, 3) AS claimed_amount_cume_dist,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status_or_unknown, adjuster_team)) AS distinct_policy_id_in_status_or_unknown_adjuster,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        NTILE(3) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id) AS claimed_amount_tercile,
        LAG(claim_date, 2) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST, id) AS prev2_claim_date,
        COUNT(*) OVER (PARTITION BY status_or_unknown) AS rows_in_status_or_unknown,
        ROW_NUMBER() OVER (PARTITION BY policy_id, status_or_unknown ORDER BY claim_date, id) AS claim_date_seq,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY status_or_unknown)) AS distinct_claim_type_in_status_or_unknown
    FROM with_parent
    WINDOW w_adjuster_id_by_claimed_amoun AS (PARTITION BY adjuster_id ORDER BY claimed_amount NULLS LAST)
),
tagged AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM with_windows
)
SELECT
    id,
    policy_id,
    claim_type,
    status_or_unknown,
    claimed_amount_share_of_status_or_unknown,
    status_or_unknown_adjuster_team_min_claimed_amou,
    next_claimed_amount,
    claimed_amount_cume_dist,
    distinct_policy_id_in_status_or_unknown_adjuster,
    first_claimed_amount,
    claimed_amount_tercile,
    prev2_claim_date,
    rows_in_status_or_unknown,
    claim_date_seq,
    distinct_claim_type_in_status_or_unknown,
    claim_type_len
FROM tagged
ORDER BY first_claimed_amount DESC
