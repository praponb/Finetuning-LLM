WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        reported_at + INTERVAL 2 HOURS AS reported_at_plus_2h
    FROM claims
),
grouped AS (
    -- Group-level totals
    SELECT
        policy_id,
        status,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount,
        COUNT(*) AS cnt
    FROM base_claims
    GROUP BY policy_id, status
    HAVING AVG(claim_type_len) > 5
),
enriched AS (
    -- Enrich with dimension data
    SELECT s.*, p.premium AS policy_premium, p.product AS policy_product
    FROM grouped s
    JOIN policies p ON p.id = s.policy_id
),
qualified AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.start_date < DATE '2025-05-30'
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.status AS policy_status,
        p.premium AS policy_premium_2,
        po.risk_score AS holder_risk_score
    FROM qualified s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
        )
      AND s.policy_id >= 20
),
unique_rows AS (
    -- Keep the latest row per policy_status
    SELECT
        policy_id,
        status,
        status_list,
        stddev_approved_amount,
        cnt,
        policy_premium,
        policy_product,
        policy_status,
        policy_premium_2,
        holder_risk_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_status ORDER BY policy_premium DESC, policy_id DESC, status DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    policy_id,
    status,
    stddev_approved_amount,
    cnt,
    policy_premium,
    policy_product,
    policy_status,
    policy_premium_2,
    holder_risk_score,
    LENGTH(TRIM(status)) AS status_len,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM unique_rows
ORDER BY status_slug DESC
