WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATE_ADD(start_date, 90) AS start_date_plus_90d,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE holder_id >= 18
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.claim_type = 'medical' THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count
    FROM base s
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(premium_bucket, status_len) AS least_premium_bucket_status_len,
        LENGTH(TRIM(product)) AS product_len
    FROM with_stats
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.premium_bucket,
        s.start_date_plus_90d,
        s.claim_claim_type_count,
        s.least_premium_bucket_status_len,
        s.product_len,
        p.risk_score AS holder_risk_score
    FROM derived s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score < 0.6
)
SELECT
    id,
    premium_bucket,
    start_date_plus_90d,
    product_len,
    DATE_FORMAT(start_date_plus_90d, 'yyyy-MM-dd') AS start_date_plus_90d_label
FROM enriched
WHERE least_premium_bucket_status_len > 1751
