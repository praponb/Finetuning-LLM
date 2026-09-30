WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE policy_id <= 4
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (ORDER BY approved_amount ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount ASC NULLS LAST), 3) AS approved_amount_cume_dist,
        SUM(approved_amount) OVER (ORDER BY approved_amount ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        LAG(claimed_amount, 1, 0) OVER (ORDER BY approved_amount ASC NULLS LAST, id) AS prev_claimed_amount,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type
    FROM recent
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.reported_at,
        s.prev_claimed_amount,
        s.rows_in_claim_type,
        a.team AS adjuster_team,
        a.level AS adjuster_level,
        a.adjuster_name,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM windowed s
    JOIN adjusters a ON a.id = s.adjuster_id
    JOIN policies p ON p.id = s.policy_id
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(prev_claimed_amount, adjuster_level) AS least_prev_claimed_amount_adjuster_level,
        YEAR(policy_start_date) AS policy_start_date_year
    FROM with_details
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(prev_claimed_amount * 1.1, 2) AS prev_claimed_amount_adj,
        QUARTER(policy_start_date) AS policy_start_date_quarter,
        DATEDIFF(policy_start_date, policy_end_date) AS days_policy_end_date_to_policy_start_date
    FROM derived
    WHERE policy_end_date BETWEEN DATE '2025-10-12' AND DATE '2026-09-19'
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', policy_start_date) AS policy_start_date_month,
        policy_start_date_year,
        DATEDIFF(MAX(policy_start_date), MIN(policy_start_date)) AS policy_start_date_span_days,
        PERCENTILE_APPROX(prev_claimed_amount_adj, 0.5) AS median_prev_claimed_amount_adj,
        ROUND(SUM(policy_start_date_quarter) / NULLIF(SUM(adjuster_level), 0), 3) AS policy_start_date_quarter_to_adjuster_level_rati,
        COUNT(*) AS row_count
    FROM enriched
    GROUP BY DATE_TRUNC('MONTH', policy_start_date), policy_start_date_year
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM totals
    WHERE policy_start_date_quarter_to_adjuster_level_rati > (SELECT MIN(policy_start_date_quarter_to_adjuster_level_rati) FROM totals)
)
-- Final projection
SELECT
    policy_start_date_month,
    policy_start_date_year,
    policy_start_date_quarter_to_adjuster_level_rati,
    row_count
FROM filtered
WHERE policy_start_date_month < TIMESTAMP '2025-02-09 09:00:00'
ORDER BY row_count DESC, policy_start_date_quarter_to_adjuster_level_rati
