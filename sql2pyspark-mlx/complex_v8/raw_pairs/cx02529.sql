WITH filtered AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM claims
    WHERE (claimed_amount BETWEEN 10673.3 AND 46461.4 OR claim_type NOT IN ('medical', 'flood'))
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        status_or_unknown,
        claim_date_quarter,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM filtered
    WHERE status_or_unknown IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        claim_date_quarter,
        claim_type_slug,
        approved_amount_bucket,
        SUM(claimed_amount) OVER (PARTITION BY TO_DATE(claim_date_quarter)) AS claim_date_quarter_to_date_sum_claimed_amount,
        NTILE(10) OVER (PARTITION BY status_or_unknown, DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount DESC, id) AS claimed_amount_decile,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_claim_date_quarter_to_date, 0), 4) AS claimed_amount_share_of_claim_date_quarter_to_da,
        COUNT(*) OVER (PARTITION BY TO_DATE(claim_date_quarter)) AS rows_in_claim_date_quarter_to_date
    FROM labeled
    WINDOW w_claim_date_quarter_to_date AS (PARTITION BY TO_DATE(claim_date_quarter))
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT claimed_amount_decile, claim_type, rows_in_claim_date_quarter_to_date
    FROM windowed
) src
PIVOT (
    MAX(rows_in_claim_date_quarter_to_date)
    FOR claim_type IN ('theft', 'medical', 'delay', 'flood')
)
