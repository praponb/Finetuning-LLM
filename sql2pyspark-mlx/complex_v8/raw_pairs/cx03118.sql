WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        ADD_MONTHS(end_date, 1) AS end_date_plus_1m
    FROM policies
    WHERE YEAR(end_date) = 2025
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM scoped_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date >= DATE '2026-01-23'
        )
),
screened_2 AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM screened s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date >= DATE '2025-01-24'
        )
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.status IN ('CANCELLED', 'LAPSED', 'ACTIVE') THEN 1 END) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_status_count,
        (SELECT ROUND(AVG(p.premium), 2) FROM policies p WHERE p.holder_id = s.holder_id) AS avg_policy_premium,
        (SELECT SUM(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS total_policy_premium
    FROM screened_2 s
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    premium_filled,
    end_date_plus_1m,
    policy_status_count,
    avg_policy_premium,
    total_policy_premium
FROM with_stats
WHERE end_date_plus_1m >= DATE '2024-05-01'
ORDER BY id DESC NULLS FIRST, start_date DESC NULLS FIRST
