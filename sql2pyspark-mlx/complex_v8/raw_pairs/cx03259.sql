WITH covered AS (
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
policy_id_stats AS (
    -- Max prior_rows_30d per policy_id
    SELECT policy_id, MAX(prior_rows_30d) AS max_prior_rows_30d
    FROM covered
    GROUP BY policy_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prior_rows_30d,
        ROUND(s.prior_rows_30d - g.max_prior_rows_30d, 2) AS prior_rows_30d_minus_max
    FROM covered s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.prior_rows_30d = g.max_prior_rows_30d
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.prior_rows_30d,
        s.prior_claimed_amount_30d,
        s.max_prior_rows_30d,
        s.prior_rows_30d_minus_max,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_type
    FROM outliers s
    JOIN claims c ON c.policy_id = s.policy_id
)
SELECT
    id,
    claim_date,
    prior_rows_30d,
    prior_claimed_amount_30d,
    max_prior_rows_30d,
    prior_rows_30d_minus_max,
    claim_id,
    GREATEST(prior_rows_30d_minus_max, claim_approved_amount) AS greatest_prior_rows_30d_minus_max_claim_approved,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
FROM detail_rows_claims
ORDER BY claim_id DESC, greatest_prior_rows_30d_minus_max_claim_approved
