WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN premium >= 3544.1 THEN 'large' WHEN premium >= 2289.0 THEN 'medium' ELSE 'small' END AS premium_tier,
        CONCAT_WS(': ', status, product) AS status_product_label
    FROM policies
    WHERE start_date IS NOT NULL
      AND status IN ('EXPIRED', 'LAPSED', 'ACTIVE')
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count
    FROM prepared_policys s
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN policy_count >= 6 THEN 'large' WHEN policy_count >= 3 THEN 'medium' ELSE 'small' END AS policy_count_tier
    FROM profiled
    WHERE product IN ('HEALTH', 'AUTO', 'HOME')
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM derived s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
      AND s.policy_count > 1
)
SELECT
    id,
    holder_id,
    product,
    status,
    premium_tier,
    status_product_label,
    policy_count,
    policy_count_tier
FROM qualified
ORDER BY holder_id, premium_tier, id
LIMIT 5
