WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_90d,
        SUM(b.claimed_amount) AS prior_claimed_amount_90d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 90)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        po.segment AS holder_segment
    FROM covered s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        prior_claimed_amount_90d,
        policy_premium,
        policy_holder_id,
        holder_segment,
        LAG(policy_premium) OVER (PARTITION BY policy_holder_id, holder_segment ORDER BY claim_date DESC, id) AS prev_policy_premium,
        FIRST_VALUE(prior_rows_90d) OVER (ORDER BY prior_rows_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prior_rows_90d
    FROM with_details
),
per_group AS (
    -- Group-level totals
    SELECT
        policy_id,
        policy_holder_id,
        MAX(first_prior_rows_90d) AS max_first_prior_rows_90d,
        MAX(claim_date) AS last_claim_date,
        MAX(prior_claimed_amount_90d) AS max_prior_claimed_amount_90d,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows
    WHERE policy_holder_id IS NOT NULL
    GROUP BY policy_id, policy_holder_id
)
-- Subtotals with GROUPING SETS
SELECT
    cnt,
    max_first_prior_rows_90d,
    policy_id,
    ROUND(SUM(max_prior_claimed_amount_90d) / NULLIF(SUM(max_first_prior_rows_90d), 0), 3) AS max_prior_claimed_amount_90d_to_max_first_prior_
FROM per_group
GROUP BY GROUPING SETS ((cnt, max_first_prior_rows_90d, policy_id), (cnt))
