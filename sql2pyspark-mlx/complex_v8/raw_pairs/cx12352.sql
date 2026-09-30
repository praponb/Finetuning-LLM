WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        ABS(premium - 2824.5) AS premium_distance,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        CONCAT_WS(' / ', product, status) AS product_status_label
    FROM policies
    WHERE status <> 'LAPSED'
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM recent_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type <> 'delay'
              AND c.status = 'REJECTED'
        )
      AND s.premium BETWEEN 2033.8 AND 4417.0
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.premium_distance,
        s.product_status_label,
        p.joined_on AS holder_joined_on,
        p.region AS holder_region
    FROM filtered s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count
    FROM joined_holder s
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(end_date, holder_joined_on) AS days_holder_joined_on_to_end_date,
        CONCAT_WS(': ', product_status_label, holder_region) AS product_status_label_holder_region_label
    FROM with_counts
)
SELECT id, metric, value
FROM (
    SELECT id, premium_distance, premium
    FROM tagged
) src
UNPIVOT (
    value FOR metric IN (premium_distance, premium)
) unp
ORDER BY id, metric
