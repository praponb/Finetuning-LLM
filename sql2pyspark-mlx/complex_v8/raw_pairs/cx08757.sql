WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week
    FROM claims
    WHERE claim_type <> 'delay'
      AND policy_id <= 23
),
unique_rows AS (
    -- Keep the latest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        claim_date_week
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date_week DESC, id DESC) AS row_num
        FROM valid_claims
    ) ranked
    WHERE row_num = 1
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.reported_at,
        s.claim_date_week,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM unique_rows s
    JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY policy_id, YEAR(claim_date) ORDER BY policy_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_date_week_month, 2) AS claimed_amount_vs_claim_date_week_month_avg,
        COUNT(*) OVER (PARTITION BY MONTH(claim_date_week)) AS rows_in_claim_date_week_month
    FROM combined_details_policy
    WINDOW w_claim_date_week_month AS (PARTITION BY MONTH(claim_date_week))
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    reported_at,
    claim_date_week,
    policy_premium,
    policy_end_date,
    claimed_amount_vs_claim_date_week_month_avg,
    ROUND(policy_premium / NULLIF(claimed_amount, 0), 3) AS policy_premium_per_claimed_amount,
    DATEDIFF(policy_end_date, claim_date) AS days_claim_date_to_policy_end_date
FROM metrics
ORDER BY claim_date_week DESC, policy_premium_per_claimed_amount DESC NULLS FIRST
