WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        UPPER(TRIM(status)) AS status_clean,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE claim_type = 'delay'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        claimed_amount_filled,
        LAG(reported_at) OVER w_all_by_claimed_amount AS prev_reported_at,
        ROW_NUMBER() OVER w_all_by_claimed_amount AS seq_no,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY status ORDER BY claim_date DESC, id) AS approved_amount_change,
        FIRST_VALUE(claimed_amount_filled) OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount_filled,
        LAST_VALUE(claimed_amount_filled) IGNORE NULLS OVER (PARTITION BY policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount_filled,
        LEAD(approved_amount) OVER w_all_by_claimed_amount AS next_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_adjuster_id_policy_id, 0), 4) AS approved_amount_share_of_adjuster_id_policy_id,
        MIN(claimed_amount) OVER (PARTITION BY status) AS status_min_claimed_amount,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount ASC), 3) AS claimed_amount_cume_dist,
        NTILE(10) OVER (ORDER BY claimed_amount ASC, id) AS claimed_amount_decile,
        MIN(approved_amount) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_min_approved_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY claim_date, id), 3) AS claim_date_percent_rank,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        RANK() OVER (PARTITION BY policy_id ORDER BY claim_date, id) AS claim_date_rank,
        RANK() OVER (ORDER BY claim_date DESC NULLS FIRST) AS claim_date_rank_2
    FROM cleaned
    WINDOW w_all_by_claimed_amount AS (ORDER BY claimed_amount ASC, id), w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(claim_type)) AS claim_type_clean,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM with_trends
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    prev_reported_at,
    last_known_claimed_amount_filled,
    next_approved_amount,
    approved_amount_share_of_adjuster_id_policy_id,
    status_min_claimed_amount,
    claimed_amount_cume_dist,
    claimed_amount_decile,
    adjuster_id_policy_id_min_approved_amount,
    last_known_reported_at,
    claim_date_rank,
    claim_date_rank_2,
    claim_type_clean,
    claim_type_len,
    ROUND(claim_date_rank / NULLIF(claimed_amount_cume_dist, 0), 3) AS claim_date_rank_per_claimed_amount_cume_dist,
    DATE_TRUNC('WEEK', claim_date) AS claim_date_week
FROM scored
WHERE claimed_amount_filled >= 7585.0
ORDER BY claimed_amount_cume_dist
