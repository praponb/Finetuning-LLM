WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_60d,
        SUM(b.claimed_amount) AS prior_claimed_amount_60d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 60)
    GROUP BY a.id, a.policy_id, a.claim_date
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        LEAST(prior_claimed_amount_60d, prior_rows_60d) AS least_prior_claimed_amount_60d_prior_rows_60d
    FROM in_window
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        prior_rows_60d,
        prior_claimed_amount_60d,
        least_prior_claimed_amount_60d_prior_rows_60d,
        ROUND(prior_rows_60d - AVG(prior_rows_60d) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 2) AS prior_rows_60d_vs_claim_date_month_avg,
        NTILE(3) OVER w_claim_date_month_policy_id_b AS least_prior_claimed_amount_60d_prior_rows_60d_te,
        ROUND(prior_rows_60d / NULLIF(SUM(prior_rows_60d) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date)), 0), 4) AS prior_rows_60d_share_of_claim_date_month
    FROM enriched
    WINDOW w_claim_date_month_policy_id_b AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id ORDER BY least_prior_claimed_amount_60d_prior_rows_60d, id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.prior_rows_60d,
        s.prior_claimed_amount_60d,
        s.least_prior_claimed_amount_60d_prior_rows_60d,
        s.prior_rows_60d_vs_claim_date_month_avg,
        s.least_prior_claimed_amount_60d_prior_rows_60d_te,
        p.product AS policy_product
    FROM windowed s
    JOIN policies p ON p.id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    prior_claimed_amount_60d,
    least_prior_claimed_amount_60d_prior_rows_60d,
    prior_rows_60d_vs_claim_date_month_avg,
    least_prior_claimed_amount_60d_prior_rows_60d_te,
    COALESCE(policy_product, 'UNKNOWN') AS policy_product_or_unknown,
    COALESCE(policy_product, 'UNKNOWN') AS policy_product_or_unknown_2
FROM with_parent
WHERE policy_id IS NOT NULL
ORDER BY id DESC NULLS FIRST, policy_product_or_unknown_2 DESC
