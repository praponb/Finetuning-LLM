WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ADD_MONTHS(end_date, 12) AS end_date_plus_12m,
        INITCAP(TRIM(product)) AS product_clean,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM policies
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        COUNT(*) FILTER (WHERE status IN ('APPROVED', 'OPEN')) AS claim_status_count
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM valid s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
SELECT *
FROM (
    SELECT status, claim_status_count, product, premium
    FROM with_counts
) src
PIVOT (
    MAX(premium)
    FOR product IN ('HEALTH', 'AUTO', 'HOME')
)
ORDER BY status, claim_status_count
