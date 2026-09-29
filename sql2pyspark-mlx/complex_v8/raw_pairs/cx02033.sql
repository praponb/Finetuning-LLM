WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        ABS(premium - 2824.5) AS premium_distance
    FROM policies
    WHERE product IS NOT NULL
      AND end_date < DATE '2026-09-19'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type IS NOT NULL) FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS last_claim_reported_at,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM base_policys s
),
latest_per_key AS (
    -- Keep the earliest row per product, status
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        status_ref,
        premium_distance,
        claim_claim_type_count,
        last_claim_reported_at,
        claim_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY last_claim_reported_at, id) AS recency_rank
        FROM with_stats
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(status, 'ALL') AS status,
    holder_id,
    claim_claim_type_count,
    MAX(premium_distance) AS max_premium_distance
FROM latest_per_key
GROUP BY GROUPING SETS ((status, holder_id, claim_claim_type_count), (status), (holder_id))
