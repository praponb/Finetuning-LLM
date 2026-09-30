WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        UPPER(TRIM(status)) AS status_clean,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE status = 'REJECTED'
      AND TO_DATE(reported_at) < DATE '2025-03-08'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(status_clean, '#', CAST(adjuster_id AS STRING)) AS status_clean_ref,
        LOWER(REGEXP_REPLACE(TRIM(claim_type_ref), ' ', '_')) AS claim_type_ref_slug,
        SUBSTRING(TRIM(status_clean), 1, 4) AS status_clean_prefix
    FROM valid
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        status,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        ROUND(100 * COUNT(CASE WHEN claim_type = 'delay' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_delay
    FROM tagged
    WHERE claim_date IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', reported_at), DATE_FORMAT(claim_date, 'yyyy-MM'), status
)
SELECT reported_at_month, claim_date_ym, claim_type_list
FROM stats
