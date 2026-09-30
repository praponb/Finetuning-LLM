WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CASE WHEN status IN ('CANCELLED', 'EXPIRED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM policies
    WHERE start_date < DATE '2025-02-13'
      AND product = 'TRAVEL'
),
status_group_stats AS (
    -- Avg premium per status_group
    SELECT status_group, AVG(premium) AS avg_premium
    FROM filtered
    GROUP BY status_group
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM filtered s
    JOIN status_group_stats g ON g.status_group = s.status_group
    WHERE s.premium > g.avg_premium
),
screened AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM outliers s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND YEAR(p.joined_on) = 2024
        )
),
latest_per_key AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        status_group,
        days_start_date_to_end_date,
        start_date_quarter,
        avg_premium,
        premium_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium_minus_avg DESC, id DESC) AS recency_rank
        FROM screened
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    product,
    premium,
    end_date,
    status,
    days_start_date_to_end_date,
    start_date_quarter,
    avg_premium,
    CASE WHEN premium_minus_avg >= 2000.0 THEN 'large' WHEN premium_minus_avg >= -500.0 THEN 'medium' ELSE 'small' END AS premium_minus_avg_tier
FROM latest_per_key
ORDER BY days_start_date_to_end_date, product DESC
