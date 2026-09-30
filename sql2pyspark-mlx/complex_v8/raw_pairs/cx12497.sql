WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        QUARTER(start_date) AS start_date_quarter,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(start_date_quarter / NULLIF(premium, 0), 3) AS start_date_quarter_per_premium,
        COALESCE(start_date_quarter, 0) AS start_date_quarter_filled
    FROM prepared
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN premium >= 3532.7 THEN 'A' WHEN premium >= 2013.8 THEN 'B' ELSE 'C' END AS premium_tier,
        holder_id,
        status,
        MAX(premium) AS max_premium,
        ROUND(AVG(start_date_quarter_filled), 2) AS avg_start_date_quarter_filled,
        COUNT(*) FILTER (WHERE status <> 'ACTIVE') AS n_status_active,
        MAX(start_date) AS last_start_date,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null
    FROM derived
    GROUP BY 1, 2, 3
),
scored AS (
    -- Label and score rows
    SELECT
        premium_tier,
        holder_id,
        status,
        max_premium,
        avg_start_date_quarter_filled,
        n_status_active,
        n_status_is_not_null,
        CASE WHEN max_premium >= 3320.0 THEN 'A' WHEN max_premium >= 2120.0 THEN 'B' ELSE 'C' END AS max_premium_tier
    FROM stats
)
-- Native SQL UNPIVOT (wide to long)
SELECT premium_tier, holder_id, metric, amount
FROM (
    SELECT premium_tier, holder_id, avg_start_date_quarter_filled, max_premium
    FROM scored
) src
UNPIVOT (
    amount FOR metric IN (avg_start_date_quarter_filled, max_premium)
) unp
