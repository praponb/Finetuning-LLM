WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount,
        UPPER(TRIM(status)) AS status_clean
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-02-24'
      AND claim_date < DATE '2025-04-25'
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        YEAR(claim_date) AS claim_date_year
    FROM valid
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.policy_id >= 2
        )
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        claimed_amount,
        status,
        approved_amount_plus_claimed_amount,
        status_clean,
        claimed_amount_filled,
        claim_date_year,
        DATE_TRUNC('HOUR', reported_at) AS reported_at_hour,
        CASE WHEN claimed_amount_filled >= 37525.0 THEN 'HIGH' WHEN claimed_amount_filled >= 10080.0 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_filled_tier,
        ROUND(claimed_amount_filled / NULLIF(claimed_amount, 0), 3) AS claimed_amount_filled_per_claimed_amount
    FROM screened
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE claimed_amount_filled > (SELECT AVG(claimed_amount_filled) FROM tagged)
)
SELECT id, metric, reading
FROM (
    SELECT id, claimed_amount, claimed_amount_filled_per_claimed_amount
    FROM screened_2
) src
UNPIVOT (
    reading FOR metric IN (claimed_amount, claimed_amount_filled_per_claimed_amount)
) unp
