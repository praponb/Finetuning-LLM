WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        CONCAT_WS('|', status, product) AS status_product_label
    FROM policies
    WHERE status IN ('LAPSED', 'CANCELLED', 'ACTIVE')
      AND premium > 1132.1
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE claim_type <> 'collision') AS claim_claim_type_count,
        COUNT(*) AS claim_count,
        COUNT(*) FILTER (WHERE status NOT IN ('OPEN', 'APPROVED')) AS claim_status_count
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_status_count, 0) AS claim_status_count
    FROM cleaned s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, claim_status_count, claim_count
    FROM with_stats
) src
UNPIVOT (
    metric_value FOR metric_name IN (claim_status_count, claim_count)
) unp
