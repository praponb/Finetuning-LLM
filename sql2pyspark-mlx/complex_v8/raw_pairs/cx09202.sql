WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ADD_MONTHS(end_date, 3) AS end_date_plus_3m,
        COALESCE(premium, 0.0) AS premium_filled,
        ABS(premium - 3465.4) AS premium_distance
    FROM policies
    WHERE end_date < DATE '2026-04-25'
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        SUM(premium_filled) AS total_premium_filled,
        MAX(end_date_plus_3m) AS last_end_date_plus_3m
    FROM filtered_policys
    WHERE holder_id IS NOT NULL
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
    HAVING AVG(premium) > 1132.1
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(total_premium_filled * 0.25, 2) AS total_premium_filled_adj,
        LOWER(TRIM(end_date_ym)) AS end_date_ym_clean
    FROM rollup_base
)
SELECT
    end_date_ym,
    total_premium_filled,
    last_end_date_plus_3m,
    total_premium_filled_adj,
    end_date_ym_clean,
    DATE_TRUNC('MONTH', last_end_date_plus_3m) AS last_end_date_plus_3m_month,
    CASE WHEN total_premium_filled >= 25530.0 THEN 'large' WHEN total_premium_filled >= 13610.0 THEN 'medium' ELSE 'small' END AS total_premium_filled_tier
FROM tagged
ORDER BY last_end_date_plus_3m_month, end_date_ym
LIMIT 5
