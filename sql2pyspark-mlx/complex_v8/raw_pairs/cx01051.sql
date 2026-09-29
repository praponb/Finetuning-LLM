WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE status IN ('ACTIVE', 'EXPIRED')
      AND product IS NOT NULL
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(start_date) AS start_date_year,
        ROUND(100 * COUNT(CASE WHEN status NOT IN ('ACTIVE', 'EXPIRED') THEN 1 END) / COUNT(*), 1) AS pct_status_not_in_active_expired,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM scoped
    GROUP BY YEAR(start_date)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY pct_status_not_in_active_expired DESC, start_date_year) AS pct_status_not_in_active_expired_decile,
        ROUND(start_date_year / NULLIF(SUM(start_date_year) OVER (PARTITION BY start_date_year), 0), 4) AS start_date_year_share_of_start_date_year
    FROM totals
),
flattened AS (
    -- One row per status_list_item
    SELECT
        s.start_date_year,
        s.pct_status_not_in_active_expired,
        s.pct_status_not_in_active_expired_decile,
        s.start_date_year_share_of_start_date_year,
        t.status_list_item
    FROM ranked s
    LATERAL VIEW EXPLODE(s.status_list) t AS status_list_item
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(start_date_year) OVER (PARTITION BY status_list_item), 2) AS status_list_item_avg_start_date_year,
        LAST_VALUE(start_date_year) OVER (PARTITION BY pct_status_not_in_active_expired_decile, status_list_item ORDER BY pct_status_not_in_active_expired, start_date_year, status_list_item ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date_year
    FROM flattened
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(TRIM(status_list_item)) AS status_list_item_clean,
        CASE WHEN start_date_year >= 2026 THEN 'large' WHEN start_date_year >= 2024 THEN 'medium' ELSE 'small' END AS start_date_year_tier
    FROM with_windows
    WHERE start_date_year > 2024
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(last_start_date_year) OVER (PARTITION BY status_list_item), 2) AS status_list_item_avg_last_start_date_year,
        ROUND(start_date_year / NULLIF(SUM(start_date_year) OVER (PARTITION BY status_list_item), 0), 4) AS start_date_year_share_of_status_list_item,
        SIZE(COLLECT_SET(start_date_year_tier) OVER w_status_list_item) AS distinct_start_date_year_tier_in_status_list_ite,
        ROUND(start_date_year - AVG(start_date_year) OVER (PARTITION BY status_list_item), 2) AS start_date_year_vs_status_list_item_avg,
        LEAD(start_date_year) OVER (ORDER BY start_date_year_share_of_start_date_year DESC, start_date_year, status_list_item) AS next_start_date_year
    FROM enriched
    WINDOW w_status_list_item AS (PARTITION BY status_list_item)
)
-- Native SQL UNPIVOT (wide to long)
SELECT start_date_year, status_list_item, measure, value
FROM (
    SELECT start_date_year, status_list_item, start_date_year_vs_status_list_item_avg, pct_status_not_in_active_expired, status_list_item_avg_start_date_year, start_date_year_share_of_status_list_item
    FROM with_trends
) src
UNPIVOT (
    value FOR measure IN (start_date_year_vs_status_list_item_avg, pct_status_not_in_active_expired, status_list_item_avg_start_date_year, start_date_year_share_of_status_list_item)
) unp
