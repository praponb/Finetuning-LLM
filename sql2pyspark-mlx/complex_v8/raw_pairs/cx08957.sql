WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        CASE WHEN status IN ('CANCELLED', 'EXPIRED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        CASE WHEN premium >= 2945.9 THEN 'A' WHEN premium >= 2319.8 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE end_date >= DATE '2026-01-20'
),
without_matches AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM recent_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.region IS NOT NULL
              AND p.joined_on < DATE '2025-07-02'
        )
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        premium,
        start_date,
        product_ref,
        status_group,
        premium_tier,
        CASE WHEN premium >= 3465.4 THEN 'HIGH' WHEN premium >= 2111.3 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier_2,
        CONCAT_WS('-', status_group, premium_tier) AS status_group_premium_tier_label
    FROM without_matches
),
latest_per_key AS (
    -- Keep the latest row per premium_tier
    SELECT
        id,
        premium,
        start_date,
        product_ref,
        status_group,
        premium_tier,
        premium_tier_2,
        status_group_premium_tier_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY premium_tier ORDER BY start_date DESC, id DESC) AS rn
        FROM tagged
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- Keep the latest row per status_group
    SELECT
        id,
        premium,
        start_date,
        product_ref,
        status_group,
        premium_tier,
        premium_tier_2,
        status_group_premium_tier_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status_group ORDER BY start_date DESC, id DESC) AS rn
        FROM latest_per_key
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    premium,
    start_date,
    product_ref,
    status_group,
    premium_tier_2,
    status_group_premium_tier_label
FROM latest
ORDER BY product_ref DESC, id
LIMIT 5
