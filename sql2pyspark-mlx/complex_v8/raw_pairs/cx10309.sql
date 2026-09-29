WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.claimed_amount) AS prior_claimed_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_parent_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_30d,
        s.prior_claimed_amount_30d,
        p.status AS policy_status,
        p.product AS policy_product
    FROM in_window s
    LEFT JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        prior_claimed_amount_30d,
        policy_status,
        policy_product,
        MAX(prior_rows_30d) OVER w_policy_id_policy_status AS policy_id_policy_status_max_prior_rows_30d,
        ROUND(prior_claimed_amount_30d - AVG(prior_claimed_amount_30d) OVER (PARTITION BY policy_id, policy_status), 2) AS prior_claimed_amount_30d_vs_policy_id_policy_sta
    FROM with_parent_policy
    WINDOW w_policy_id_policy_status AS (PARTITION BY policy_id, policy_status)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced
    WHERE prior_claimed_amount_30d_vs_policy_id_policy_sta > (SELECT MIN(prior_claimed_amount_30d_vs_policy_id_policy_sta) FROM sequenced)
)
SELECT
    id,
    policy_status,
    policy_product,
    policy_id_policy_status_max_prior_rows_30d,
    prior_claimed_amount_30d_vs_policy_id_policy_sta
FROM with_activity
ORDER BY policy_status, policy_product DESC, id
LIMIT 20
