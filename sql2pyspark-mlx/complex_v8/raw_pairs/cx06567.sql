WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        QUARTER(reported_at) AS reported_at_quarter,
        QUARTER(reported_at) AS reported_at_quarter_2
    FROM claims
    WHERE claimed_amount BETWEEN 13635.1 AND 31752.9
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at_quarter,
        reported_at_quarter_2,
        ROUND(AVG(claimed_amount) OVER w_policy_id_reported_at_quarte, 2) AS policy_id_reported_at_quarter_2_avg_claimed_amou,
        MIN(claimed_amount) OVER (PARTITION BY reported_at_quarter_2 ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far,
        MAX(claimed_amount) OVER w_policy_id_reported_at_quarte AS policy_id_reported_at_quarter_2_max_claimed_amou,
        ROUND(AVG(reported_at_quarter_2) OVER (PARTITION BY reported_at_quarter_2 ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS reported_at_quarter_2_avg_7
    FROM base_claims
    WINDOW w_policy_id_reported_at_quarte AS (PARTITION BY policy_id, reported_at_quarter_2)
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.premium AS policy_premium,
        p.status AS policy_status,
        po.region AS holder_region
    FROM metrics s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    id,
    claimed_amount,
    approved_amount,
    status,
    reported_at_quarter,
    policy_id_reported_at_quarter_2_avg_claimed_amou,
    min_claimed_amount_so_far,
    policy_id_reported_at_quarter_2_max_claimed_amou,
    reported_at_quarter_2_avg_7,
    policy_premium,
    policy_status,
    holder_region,
    CASE WHEN reported_at_quarter >= 3 THEN 'HIGH' WHEN reported_at_quarter >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS reported_at_quarter_tier
FROM combined_details_policy
