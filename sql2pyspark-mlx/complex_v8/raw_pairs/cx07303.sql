WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.adjuster_id,
        s.premium,
        p.status AS policy_status,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id
    FROM in_window s
    JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY policy_id, policy_status)) AS distinct_adjuster_id_in_policy_id_policy_status,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id, adjuster_id ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        SIZE(COLLECT_SET(policy_holder_id) OVER (PARTITION BY policy_id, policy_status)) AS distinct_policy_holder_id_in_policy_id_policy_st,
        NTILE(3) OVER w_policy_id_adjuster_id_by_pre_2 AS premium_tercile
    FROM with_details
    WINDOW w_policy_id_adjuster_id_by_pre_2 AS (PARTITION BY policy_id, adjuster_id ORDER BY premium, id)
)
SELECT
    id,
    policy_id,
    claimed_amount,
    adjuster_id,
    premium,
    policy_status,
    policy_premium,
    policy_holder_id,
    distinct_adjuster_id_in_policy_id_policy_status,
    last_known_claimed_amount,
    SUBSTRING(TRIM(policy_status), 1, 3) AS policy_status_prefix,
    CONCAT(policy_status, '#', CAST(premium_tercile AS STRING)) AS policy_status_ref
FROM ranked
ORDER BY adjuster_id DESC, id
LIMIT 5
