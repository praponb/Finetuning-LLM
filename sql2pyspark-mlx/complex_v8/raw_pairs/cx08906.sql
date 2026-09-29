WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.claimed_amount) AS prior_claimed_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        policy_id,
        prior_rows_180d,
        YEAR(claim_date) AS claim_date_year,
        COUNT(*) AS n_rows,
        ROUND(SUM(prior_claimed_amount_180d), 2) AS total_prior_claimed_amount_180d,
        SUM(prior_claimed_amount_180d) AS total_prior_claimed_amount_180d_2,
        SUM(prior_rows_180d) AS total_prior_rows_180d
    FROM matched
    GROUP BY policy_id, prior_rows_180d, YEAR(claim_date)
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE status = 'IN_REVIEW') AS claim_status_count,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.last_claim_reported_at
    FROM rollup_base s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.policy_id,
        s.prior_rows_180d,
        s.claim_date_year,
        s.total_prior_claimed_amount_180d_2,
        s.total_prior_rows_180d,
        s.claim_status_count,
        s.last_claim_reported_at,
        p.end_date AS policy_end_date,
        po.region AS holder_region
    FROM with_counts s
    LEFT JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
)
SELECT
    policy_id,
    prior_rows_180d,
    claim_date_year,
    total_prior_claimed_amount_180d_2,
    policy_end_date
FROM enriched
