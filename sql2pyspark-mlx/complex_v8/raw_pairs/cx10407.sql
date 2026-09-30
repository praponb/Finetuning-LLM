WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE (claimed_amount < 34401.0 OR YEAR(claim_date) = 2024)
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        MIN(claimed_amount) AS min_claimed_amount,
        COUNT(*) FILTER (WHERE status IN ('APPROVED', 'PAID', 'OPEN')) AS n_status_in_approved_paid_open
    FROM cleaned_claims
    GROUP BY DATE_TRUNC('MONTH', claim_date)
    HAVING SUM(claimed_amount) > 5313.8
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(min_claimed_amount / NULLIF(SUM(min_claimed_amount) OVER w_claim_date_month_to_date, 0), 4) AS min_claimed_amount_share_of_claim_date_month_to_,
        FIRST_VALUE(min_claimed_amount) OVER (ORDER BY n_status_in_approved_paid_open DESC, claim_date_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_claimed_amount,
        MIN(min_claimed_amount) OVER w_claim_date_month_to_date AS claim_date_month_to_date_min_min_claimed_amount,
        COUNT(*) OVER (PARTITION BY TO_DATE(claim_date_month)) AS rows_in_claim_date_month_to_date
    FROM per_group
    WINDOW w_claim_date_month_to_date AS (PARTITION BY TO_DATE(claim_date_month))
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE claim_date_month_to_date_min_min_claimed_amount > (SELECT AVG(claim_date_month_to_date_min_min_claimed_amount) FROM windowed)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(min_claimed_amount, 0.0) AS min_claimed_amount_filled,
        DATE_TRUNC('MONTH', claim_date_month) AS claim_date_month_month
    FROM qualified
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY TO_DATE(claim_date_month_month) ORDER BY min_claimed_amount_share_of_claim_date_month_to_ DESC NULLS FIRST, claim_date_month) AS min_claimed_amount_share_of_claim_date_month_to__2,
        MIN(min_claimed_amount) OVER (PARTITION BY n_status_in_approved_paid_open, MONTH(claim_date_month_month)) AS n_status_in_approved_paid_open_claim_date_month_,
        ROUND(min_claimed_amount_filled / NULLIF(SUM(min_claimed_amount_filled) OVER (PARTITION BY n_status_in_approved_paid_open, MONTH(claim_date_month_month)), 0), 4) AS min_claimed_amount_filled_share_of_n_status_in_a,
        NTILE(3) OVER (PARTITION BY TO_DATE(claim_date_month_month) ORDER BY min_claimed_amount_share_of_claim_date_month_to_ DESC NULLS FIRST, claim_date_month) AS min_claimed_amount_share_of_claim_date_month_to__3
    FROM tagged
)
SELECT
    claim_date_month,
    min_claimed_amount,
    n_status_in_approved_paid_open,
    min_claimed_amount_share_of_claim_date_month_to_,
    first_min_claimed_amount,
    rows_in_claim_date_month_to_date,
    claim_date_month_month,
    n_status_in_approved_paid_open_claim_date_month_,
    min_claimed_amount_share_of_claim_date_month_to__3
FROM with_trends
ORDER BY rows_in_claim_date_month_to_date, n_status_in_approved_paid_open DESC
