WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        YEAR(claim_date) AS claim_date_year,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE claimed_amount BETWEEN 13635.1 AND 41760.8
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        claim_date,
        reported_at,
        claim_date_year,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY claim_date_year, DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROW_NUMBER() OVER (PARTITION BY claim_date_year, DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount, id) AS claimed_amount_seq,
        ROW_NUMBER() OVER w_claim_date_year_by_greatest AS greatest_claimed_amount_approved_amount_seq
    FROM cleaned_claims
    WINDOW w_claim_date_year_by_greatest AS (PARTITION BY claim_date_year ORDER BY greatest_claimed_amount_approved_amount DESC, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', second_reported_at) AS second_reported_at_month,
        claim_date_year,
        greatest_claimed_amount_approved_amount_seq,
        MIN(reported_at) AS first_reported_at,
        MAX(reported_at) AS last_reported_at
    FROM with_windows
    WHERE claimed_amount_seq < 9
    GROUP BY DATE_TRUNC('MONTH', second_reported_at), claim_date_year, greatest_claimed_amount_approved_amount_seq
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE greatest_claimed_amount_approved_amount_seq < (SELECT MAX(greatest_claimed_amount_approved_amount_seq) FROM grouped)
)
-- Final projection
SELECT
    second_reported_at_month,
    claim_date_year,
    greatest_claimed_amount_approved_amount_seq,
    first_reported_at,
    last_reported_at,
    COALESCE(claim_date_year, 0) AS claim_date_year_filled
FROM filtered
ORDER BY first_reported_at DESC, second_reported_at_month DESC
