WITH recent_claims AS (
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
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-25 13:00:00'
),
with_parent_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.status_slug,
        p.holder_id AS policy_holder_id,
        p.status AS policy_status
    FROM recent_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        status_slug,
        policy_holder_id,
        policy_status,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY status, claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        RANK() OVER (ORDER BY claimed_amount NULLS LAST) AS claimed_amount_rank,
        ROUND(PERCENT_RANK() OVER w_policy_status_claim_type_by, 3) AS approved_amount_percent_rank,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_holder_id), 2) AS policy_holder_id_avg_approved_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_status), 2) AS policy_status_avg_claimed_amount,
        COUNT(*) OVER (PARTITION BY policy_status) AS rows_in_policy_status,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount NULLS LAST), 3) AS claimed_amount_cume_dist,
        FIRST_VALUE(approved_amount) OVER (ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_holder_id ORDER BY approved_amount), 3) AS approved_amount_percent_rank_2
    FROM with_parent_policy
    WINDOW w_policy_status_claim_type_by AS (PARTITION BY policy_status, claim_type ORDER BY approved_amount DESC)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM sequenced
    WHERE claimed_amount_rank = 1
      AND adjuster_id IS NOT NULL
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    status_slug,
    policy_holder_id,
    policy_status,
    second_approved_amount,
    approved_amount_percent_rank,
    policy_holder_id_avg_approved_amount,
    policy_status_avg_claimed_amount,
    rows_in_policy_status,
    claimed_amount_cume_dist,
    approved_amount_percent_rank_2,
    COALESCE(rows_in_policy_status, 0) AS rows_in_policy_status_filled,
    ROUND(second_approved_amount * 0.9, 2) AS second_approved_amount_adj
FROM top_rows
ORDER BY approved_amount DESC NULLS FIRST
