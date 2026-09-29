WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE holder_id <= 16
      AND start_date >= DATE '2024-05-07'
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.days_end_date_to_start_date,
        s.premium_filled,
        p.joined_on AS holder_joined_on
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium <= 2945.9
              AND p.product NOT IN ('TRAVEL', 'LIFE')
        )
),
latest AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        days_end_date_to_start_date,
        premium_filled,
        holder_joined_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY start_date DESC, id DESC) AS recency_rank
        FROM without_matches
    ) ranked
    WHERE recency_rank = 1
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        holder_id,
        MAX(premium_filled) AS max_premium_filled,
        MAX(start_date) AS last_start_date,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(end_date) AS first_end_date
    FROM latest
    WHERE holder_joined_on IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', end_date), DATE_FORMAT(start_date, 'yyyy-MM'), holder_id
    HAVING SUM(days_end_date_to_start_date) > -560
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.end_date_month,
        s.start_date_ym,
        s.holder_id,
        s.max_premium_filled,
        s.last_start_date,
        s.row_count,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM summary s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_details_holder
    WHERE row_count < (SELECT MAX(row_count) FROM with_details_holder)
)
SELECT *
FROM (
    SELECT last_start_date, holder_segment, holder_risk_score
    FROM qualified
) src
PIVOT (
    COUNT(holder_risk_score)
    FOR holder_segment IN ('VIP', 'SME', 'RETAIL')
)
ORDER BY last_start_date
