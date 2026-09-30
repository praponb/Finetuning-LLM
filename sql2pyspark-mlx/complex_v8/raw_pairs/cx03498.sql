WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE holder_id IS NOT NULL
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM filtered_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount >= 4868.7
              AND c.adjuster_id IS NOT NULL
        )
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        MONTH(end_date) AS end_date_month,
        COALESCE(premium_filled, 0.0) AS premium_filled_filled
    FROM qualified
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(end_date_month / NULLIF(premium_filled, 0), 3) AS end_date_month_per_premium_filled
    FROM labeled
),
holder_id_stats AS (
    -- Avg end_date_month per holder_id
    SELECT holder_id, AVG(end_date_month) AS avg_end_date_month
    FROM enriched
    GROUP BY holder_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_end_date_month,
        ROUND(s.end_date_month - g.avg_end_date_month, 2) AS end_date_month_minus_avg
    FROM enriched s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.end_date_month > g.avg_end_date_month
)
SELECT
    status,
    end_date_month,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    COUNT(*) FILTER (WHERE status NOT IN ('ACTIVE', 'EXPIRED')) AS n_status_not_in_active_expired
FROM outliers
GROUP BY GROUPING SETS ((status, end_date_month), (status), ())
ORDER BY status NULLS LAST, end_date_month NULLS LAST
