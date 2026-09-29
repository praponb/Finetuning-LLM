WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        CASE WHEN claim_type IN ('medical', 'theft') THEN 'group_a' WHEN claim_type = 'flood' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        DATE_TRUNC('HOUR', reported_at) AS reported_at_hour
    FROM claims
    WHERE claimed_amount < 34763.4
),
labeled AS (
    -- Label and score rows
    SELECT *, QUARTER(reported_at_hour) AS reported_at_hour_quarter
    FROM scoped_claims
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_type, reported_at_hour_quarter) AS rows_in_claim_type_reported_at_hour_quarter,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_claim_date_year_by_claim_dat) AS days_since_prev_claim_date,
        SIZE(COLLECT_SET(policy_id) OVER w_claim_type_reported_at_hour) AS distinct_policy_id_in_claim_type_reported_at_hou,
        MAX(claimed_amount) OVER (PARTITION BY claim_type, reported_at_hour_quarter) AS claim_type_reported_at_hour_quarter_max_claimed_,
        ROUND(reported_at_hour_quarter / NULLIF(SUM(reported_at_hour_quarter) OVER (PARTITION BY claim_type, reported_at_hour_quarter), 0), 4) AS reported_at_hour_quarter_share_of_claim_type_rep
    FROM labeled
    WINDOW w_claim_date_year_by_claim_dat AS (PARTITION BY YEAR(claim_date) ORDER BY claim_date ASC, id), w_claim_type_reported_at_hour AS (PARTITION BY claim_type, reported_at_hour_quarter)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    status,
    claim_type_group,
    reported_at_hour,
    reported_at_hour_quarter,
    rows_in_claim_type_reported_at_hour_quarter,
    days_since_prev_claim_date,
    claim_type_reported_at_hour_quarter_max_claimed_,
    reported_at_hour_quarter_share_of_claim_type_rep
FROM sequenced
WHERE rows_in_claim_type_reported_at_hour_quarter BETWEEN 5 AND 14
ORDER BY days_since_prev_claim_date, id
LIMIT 5
