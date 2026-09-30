WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        FLOOR(premium / 1000) * 1000 AS premium_bucket_2,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE premium >= 2013.8
      AND status <> 'LAPSED'
),
annotated AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(status_or_unknown), 1, 2) AS status_or_unknown_prefix
    FROM prepared
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        s.premium_bucket_2,
        s.status_or_unknown_prefix,
        c.id AS claim_id,
        c.claim_date,
        c.claim_type
    FROM annotated s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    premium,
    premium_bucket_2,
    status_or_unknown_prefix,
    claim_id,
    claim_date,
    claim_type
FROM exploded_detail_claims
ORDER BY status_or_unknown_prefix NULLS LAST
