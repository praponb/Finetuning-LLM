WITH base_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE (policy_id IS NOT NULL OR MONTH(reported_at) IN (2, 3))
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base_claims s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.reported_at < TIMESTAMP '2025-02-18 14:00:00'
        )
),
deduped AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claim_date_dayofweek,
        status_slug
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY claim_date_dayofweek DESC, id DESC) AS recency_rank
        FROM qualified
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    claim_date_dayofweek,
    claim_type,
    COUNT(*) AS n_rows,
    ROUND(100 * COUNT(CASE WHEN claim_type = 'fire' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_fire
FROM deduped
GROUP BY ROLLUP(claim_date_dayofweek, claim_type)
ORDER BY claim_date_dayofweek NULLS LAST, claim_type NULLS LAST
