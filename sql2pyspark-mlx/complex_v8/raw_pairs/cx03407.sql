WITH cleaned_claims AS (
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
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE status IS NOT NULL
),
enriched AS (
    -- Add calculated columns
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
        claim_type_len,
        status_claim_type_label,
        ADD_MONTHS(claim_date, 12) AS claim_date_plus_12m,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM cleaned_claims
),
combined_details_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.claim_type_len,
        s.status_claim_type_label,
        s.reported_at_label,
        s.approved_amount_bucket,
        p.product AS policy_product,
        a.team AS adjuster_team,
        a.adjuster_name
    FROM enriched s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
claim_type_stats AS (
    -- Avg claim_type_len per claim_type
    SELECT claim_type, AVG(claim_type_len) AS avg_claim_type_len
    FROM combined_details_policy
    GROUP BY claim_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_claim_type_len,
        ROUND(s.claim_type_len - g.avg_claim_type_len, 2) AS claim_type_len_minus_avg
    FROM combined_details_policy s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.claim_type_len > g.avg_claim_type_len
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(policy_product, 'ALL') AS policy_product,
    COALESCE(adjuster_team, 'ALL') AS adjuster_team,
    GROUPING(policy_product) AS is_total_policy_product,
    COUNT(*) AS cnt,
    ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount
FROM outliers
GROUP BY GROUPING SETS ((policy_product, adjuster_team), (policy_product), (adjuster_team), ())
