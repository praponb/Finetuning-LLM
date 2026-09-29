WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
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
    WHERE claimed_amount >= 14019.8
      AND adjuster_id >= 4
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label
    FROM recent_claims
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(status)) AS status_len,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM scored
),
policy_id_stats AS (
    -- Max days_since_claim_date per policy_id
    SELECT policy_id, MAX(days_since_claim_date) AS max_days_since_claim_date
    FROM tagged
    GROUP BY policy_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_claim_date,
        ROUND(s.days_since_claim_date - g.max_days_since_claim_date, 2) AS days_since_claim_date_minus_max
    FROM tagged s
    JOIN policy_id_stats g ON g.policy_id = s.policy_id
    WHERE s.days_since_claim_date = g.max_days_since_claim_date
)
-- Subtotals with ROLLUP
SELECT
    status_len,
    claim_type,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
FROM above_average
GROUP BY ROLLUP(status_len, claim_type)
ORDER BY status_len NULLS LAST, claim_type NULLS LAST
