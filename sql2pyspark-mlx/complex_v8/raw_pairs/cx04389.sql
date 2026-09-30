WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at
    FROM claims
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        days_since_reported_at,
        LEAST(approved_amount, days_since_reported_at) AS least_approved_amount_days_since_reported_at,
        LOWER(TRIM(claim_type)) AS claim_type_clean,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM filtered_claims
    WHERE approved_amount < 14993.0
),
latest AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        days_since_reported_at,
        least_approved_amount_days_since_reported_at,
        claim_type_clean,
        status_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id DESC) AS rn
        FROM annotated
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM latest
    WHERE days_since_reported_at > (SELECT AVG(days_since_reported_at) FROM latest)
),
annotated_2 AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS(': ', status_or_unknown, claim_type_clean) AS status_or_unknown_claim_type_clean_label
    FROM qualified
)
SELECT id, metric, amount
FROM (
    SELECT id, approved_amount, least_approved_amount_days_since_reported_at
    FROM annotated_2
) src
UNPIVOT (
    amount FOR metric IN (approved_amount, least_approved_amount_days_since_reported_at)
) unp
