WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        ROUND(premium * 0.9, 2) AS premium_adj,
        DATE_ADD(start_date, 7) AS start_date_plus_7d
    FROM policies
    WHERE end_date >= DATE '2025-10-12'
),
status_stats AS (
    -- Avg premium per status
    SELECT status, AVG(premium) AS avg_premium
    FROM scoped_policys
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM scoped_policys s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium > g.avg_premium
)
SELECT
    COALESCE(status, 'ALL') AS status,
    holder_id,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(premium_adj) AS max_premium_adj
FROM above_average
GROUP BY GROUPING SETS ((status, holder_id), (status), ())
