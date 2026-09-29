WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.approved_amount) AS prior_approved_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
rollup_base AS (
    -- Summarise per group
    SELECT
        policy_id,
        YEAR(claim_date) AS claim_date_year,
        prior_rows_60d,
        ROUND(SUM(prior_approved_amount_60d), 2) AS total_prior_approved_amount_60d,
        ROUND(SUM(prior_rows_60d) / NULLIF(SUM(prior_approved_amount_60d), 0), 3) AS prior_rows_60d_to_prior_approved_amount_60d_rati,
        MIN(claim_date) AS first_claim_date,
        ROUND(AVG(prior_approved_amount_60d), 2) AS avg_prior_approved_amount_60d,
        ROUND(STDDEV(prior_rows_60d), 2) AS stddev_prior_rows_60d
    FROM covered
    GROUP BY policy_id, YEAR(claim_date), prior_rows_60d
    HAVING SUM(prior_rows_60d) > 1
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.policy_id,
        s.claim_date_year,
        s.prior_rows_60d,
        s.total_prior_approved_amount_60d,
        s.stddev_prior_rows_60d,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at
    FROM rollup_base s
    JOIN claims c ON c.policy_id = s.policy_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(claim_date_year, claim_claimed_amount) AS greatest_claim_date_year_claim_claimed_amount,
        DATE_TRUNC('WEEK', claim_reported_at) AS claim_reported_at_week
    FROM detail_rows_claims
    WHERE claim_claimed_amount < 40638.4
)
-- Final projection
SELECT
    policy_id,
    claim_date_year,
    prior_rows_60d,
    total_prior_approved_amount_60d,
    stddev_prior_rows_60d,
    claim_claimed_amount,
    claim_approved_amount,
    greatest_claim_date_year_claim_claimed_amount,
    claim_reported_at_week
FROM enriched
ORDER BY stddev_prior_rows_60d
