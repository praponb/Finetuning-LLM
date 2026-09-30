WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE YEAR(end_date) = 2026
),
with_counts AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.holder_id) AS avg_policy_premium,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count_2
    FROM valid_policys s
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        policy_count,
        policy_count_2,
        LOWER(TRIM(status)) AS status_clean,
        COALESCE(avg_policy_premium, 0.0) AS avg_policy_premium_filled,
        CASE WHEN premium >= 3817.5 THEN 'large' WHEN premium >= 2033.8 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM with_counts
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date BETWEEN DATE '2024-06-07' AND DATE '2025-11-15'
        )
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    policy_count,
    policy_count_2,
    premium_tier,
    INITCAP(TRIM(premium_tier)) AS premium_tier_clean
FROM filtered
WHERE policy_count_2 BETWEEN 2 AND 6
ORDER BY id NULLS LAST, policy_count NULLS LAST
