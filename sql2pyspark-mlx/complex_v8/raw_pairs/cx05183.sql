WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE product IN ('TRAVEL', 'HEALTH', 'LIFE')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') AS start_date_label,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        UPPER(TRIM(status)) AS status_clean
    FROM base
    WHERE status NOT IN ('EXPIRED', 'LAPSED')
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(premium / NULLIF(SUM(premium) OVER w_start_date_month, 0), 4) AS premium_share_of_start_date_month,
        DATEDIFF(start_date, LAG(start_date) OVER (ORDER BY premium DESC, id)) AS days_since_prev_start_date,
        COUNT(*) OVER w_start_date_month AS rows_in_start_date_month,
        NTH_VALUE(premium, 2) OVER (ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(start_date))) AS distinct_status_in_start_date_month,
        ROUND(AVG(premium) OVER (PARTITION BY MONTH(start_date)), 2) AS start_date_month_avg_premium
    FROM enriched
    WINDOW w_start_date_month AS (PARTITION BY MONTH(start_date))
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        TRUNC(start_date, 'MM') AS start_date_month_start,
        CASE WHEN second_premium >= 3544.1 THEN 'A' WHEN second_premium >= 2013.8 THEN 'B' ELSE 'C' END AS second_premium_tier
    FROM sequenced
    WHERE start_date >= DATE '2024-12-03'
)
SELECT
    distinct_status_in_start_date_month,
    holder_id,
    second_premium_tier,
    GROUPING(distinct_status_in_start_date_month) AS is_total_distinct_status_in_start_date_month,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids
FROM labeled
GROUP BY GROUPING SETS ((distinct_status_in_start_date_month, holder_id, second_premium_tier), (distinct_status_in_start_date_month))
ORDER BY distinct_status_in_start_date_month NULLS LAST, holder_id NULLS LAST, second_premium_tier NULLS LAST
