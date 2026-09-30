WITH recent_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount,
        CASE WHEN claim_type IN ('collision', 'medical') THEN 'group_a' WHEN claim_type = 'delay' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE approved_amount >= 0.0
      AND policy_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id), 2) AS approved_amount_vs_policy_id_avg,
        MAX(approved_amount) OVER (PARTITION BY claim_type_group, policy_id ORDER BY approved_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS approved_amount_max_7,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id)) AS distinct_status_in_policy_id
    FROM recent_claims
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM windowed s
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.approved_amount_plus_claimed_amount,
        s.approved_amount_vs_policy_id_avg,
        s.approved_amount_max_7,
        s.distinct_status_in_policy_id,
        s.claim_count,
        s.claim_count_2,
        p.holder_id AS policy_holder_id,
        po.segment AS holder_segment,
        po.joined_on AS holder_joined_on
    FROM with_stats s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
    WHERE p.holder_id IS NOT NULL
),
with_parent AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.holder_id AS policy_holder_id_2,
        p.status AS policy_status,
        po.segment AS holder_segment_2
    FROM combined_details_policy s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = s.policy_holder_id
)
SELECT
    id,
    policy_id,
    adjuster_id,
    approved_amount,
    status,
    reported_at,
    approved_amount_plus_claimed_amount,
    approved_amount_vs_policy_id_avg,
    approved_amount_max_7,
    distinct_status_in_policy_id,
    claim_count,
    claim_count_2,
    policy_holder_id,
    holder_segment,
    holder_joined_on,
    policy_holder_id_2,
    policy_status,
    holder_segment_2,
    SUBSTRING(TRIM(holder_segment_2), 1, 2) AS holder_segment_2_prefix,
    CASE WHEN holder_segment_2 IN ('RETAIL', 'VIP') THEN 'group_a' WHEN holder_segment_2 = 'SME' THEN 'group_b' ELSE 'other' END AS holder_segment_2_group
FROM with_parent
WHERE policy_holder_id_2 >= 6
ORDER BY approved_amount, id
LIMIT 20
