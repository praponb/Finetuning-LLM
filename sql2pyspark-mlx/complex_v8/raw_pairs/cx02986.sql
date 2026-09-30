WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
    WHERE start_date >= DATE '2024-07-19'
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM recent_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.risk_score BETWEEN 0.1 AND 0.8
              AND p.region <> 'South'
        )
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS max_policy_premium,
        (SELECT SUM(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS total_policy_premium,
        (SELECT MIN(p.premium) FROM policies p WHERE p.holder_id = s.holder_id) AS min_policy_premium
    FROM filtered s
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(max_policy_premium) OVER (PARTITION BY holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_policy_premium,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY status, YEAR(start_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status, product)) AS distinct_holder_id_in_status_product,
        SUM(max_policy_premium) OVER (PARTITION BY status, YEAR(start_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_policy_premium_2
    FROM summarized_rows
    WINDOW w_status_product AS (PARTITION BY status, product)
)
SELECT id, measure, reading
FROM (
    SELECT id, running_max_policy_premium_2, premium
    FROM with_windows
) src
UNPIVOT (
    reading FOR measure IN (running_max_policy_premium_2 AS m_running_max_policy_premium_2, premium AS m_premium)
) unp
