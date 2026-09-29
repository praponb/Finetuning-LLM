WITH filtered AS (
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
        reported_at,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM claims
    WHERE claim_type = 'theft'
      AND policy_id IS NOT NULL
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_date,
        (SELECT MIN(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_date_2
    FROM filtered s
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        DATE_TRUNC('MONTH', first_claim_date_2) AS first_claim_date_2_month,
        adjuster_id,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        SUM(claimed_amount) FILTER (WHERE status = 'OPEN') AS claimed_amount_status_open,
        COUNT(DISTINCT claim_type) AS distinct_claim_types
    FROM summarized_rows
    WHERE adjuster_id IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', reported_at), DATE_TRUNC('MONTH', first_claim_date_2), adjuster_id
)
SELECT
    reported_at_month,
    first_claim_date_2_month,
    adjuster_id,
    claim_date_span_days,
    claimed_amount_status_open,
    distinct_claim_types,
    ROUND((UNIX_TIMESTAMP(reported_at_month) - UNIX_TIMESTAMP(first_claim_date_2_month)) / 3600, 1) AS hours_first_claim_date_2_month_to_reported_at_mo,
    first_claim_date_2_month + INTERVAL 1 DAYS AS first_claim_date_2_month_plus_1d
FROM per_group
WHERE HOUR(reported_at_month) BETWEEN 6 AND 11
