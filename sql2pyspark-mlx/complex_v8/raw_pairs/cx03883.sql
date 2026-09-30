WITH recent_claims AS (
    -- Filter claims
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
        CASE WHEN claim_type IN ('flood', 'collision') THEN 'group_a' WHEN claim_type = 'medical' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(reported_at) IGNORE NULLS OVER (ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_reported_at,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY claim_type_group, MONTH(reported_at))) AS distinct_claim_type_in_claim_type_group_reported,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_adjuster_id_by_approved_amou) AS days_since_prev_claim_date,
        ROUND(AVG(approved_amount) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status), 2) AS approved_amount_vs_status_avg,
        FIRST_VALUE(approved_amount) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        LAST_VALUE(approved_amount) OVER (PARTITION BY policy_id, status ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        LAG(approved_amount, 3) OVER w_adjuster_id_by_approved_amou AS prev3_approved_amount
    FROM recent_claims
    WINDOW w_adjuster_id_by_approved_amou AS (PARTITION BY adjuster_id ORDER BY approved_amount DESC NULLS FIRST, id)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(days_since_prev_claim_date) OVER w_distinct_claim_type_in_claim AS distinct_claim_type_in_claim_type_group_reported_2,
        ROUND(days_since_prev_claim_date - AVG(days_since_prev_claim_date) OVER (PARTITION BY distinct_claim_type_in_claim_type_group_reported, claim_type_group), 2) AS days_since_prev_claim_date_vs_distinct_claim_typ,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY distinct_claim_type_in_claim_type_group_reported, claim_type_group), 2) AS distinct_claim_type_in_claim_type_group_reported_4,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY claim_type_group, policy_id ORDER BY first_known_reported_at DESC, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROW_NUMBER() OVER (ORDER BY distinct_claim_type_in_claim_type_group_reported ASC, id) AS distinct_claim_type_in_claim_type_group_reported_5
    FROM sequenced
    WINDOW w_distinct_claim_type_in_claim AS (PARTITION BY distinct_claim_type_in_claim_type_group_reported, claim_type_group)
)
-- Final projection
SELECT
    id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    claim_type_group,
    first_known_reported_at,
    approved_amount_avg_3_prior,
    approved_amount_vs_status_avg,
    last_approved_amount,
    prev3_approved_amount,
    distinct_claim_type_in_claim_type_group_reported_2,
    days_since_prev_claim_date_vs_distinct_claim_typ,
    distinct_claim_type_in_claim_type_group_reported_4,
    minutes_since_prev_reported_at,
    distinct_claim_type_in_claim_type_group_reported_5,
    ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(first_known_reported_at)) / 3600, 1) AS hours_first_known_reported_at_to_reported_at
FROM with_windows
WHERE TO_DATE(reported_at) < DATE '2025-03-14'
ORDER BY distinct_claim_type_in_claim_type_group_reported_4 DESC, status NULLS LAST, id
LIMIT 5
