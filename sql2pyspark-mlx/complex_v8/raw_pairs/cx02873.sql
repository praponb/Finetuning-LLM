WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        INITCAP(TRIM(product)) AS product_clean
    FROM policies
    WHERE premium > 1051.9
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS max_policy_premium
    FROM valid_policys s
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_id, start_date, status, premium
    FROM with_stats
) src
PIVOT (
    SUM(premium) AS amount, MAX(premium) AS max_v
    FOR status IN ('EXPIRED', 'CANCELLED')
)
