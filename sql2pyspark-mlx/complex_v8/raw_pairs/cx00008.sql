WITH recent AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        INITCAP(TRIM(tariff)) AS tariff_clean,
        ABS(rate - 5.5) AS rate_distance
    FROM tariff_windows
    WHERE TO_DATE(valid_to) < DATE '2025-03-12'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        TO_DATE(valid_from) AS valid_from_day,
        CASE WHEN rate >= 5.9 THEN 'HIGH' WHEN rate >= 3.7 THEN 'MEDIUM' ELSE 'LOW' END AS rate_tier,
        DATE_FORMAT(valid_to, 'yyyy-MM') AS valid_to_ym,
        MAX(rate) AS max_rate,
        ROUND(AVG(rate_distance), 2) AS avg_rate_distance,
        COUNT(*) AS row_count,
        MIN(valid_to) AS first_valid_to,
        MAX(valid_to) AS last_valid_to
    FROM recent
    GROUP BY 1, 2, 3
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN avg_rate_distance >= 4.8 THEN 'large' WHEN avg_rate_distance >= 1.6 THEN 'medium' ELSE 'small' END AS avg_rate_distance_tier,
        TO_DATE(first_valid_to) AS first_valid_to_date
    FROM rollup_base
),
tagged AS (
    -- Add calculated columns
    SELECT
        valid_from_day,
        rate_tier,
        valid_to_ym,
        max_rate,
        avg_rate_distance,
        first_valid_to,
        last_valid_to,
        first_valid_to_date,
        TO_DATE(last_valid_to) AS last_valid_to_date
    FROM scored
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        valid_from_day,
        rate_tier,
        valid_to_ym,
        max_rate,
        avg_rate_distance,
        first_valid_to,
        last_valid_to,
        first_valid_to_date,
        last_valid_to_date,
        ROUND(avg_rate_distance - AVG(avg_rate_distance) OVER w_first_valid_to_date_year, 2) AS avg_rate_distance_vs_first_valid_to_date_year_av,
        MIN(avg_rate_distance) OVER (PARTITION BY YEAR(first_valid_to_date)) AS first_valid_to_date_year_min_avg_rate_distance,
        ROUND(AVG(max_rate) OVER (PARTITION BY DATE_TRUNC('MONTH', last_valid_to_date) ORDER BY last_valid_to, valid_from_day, rate_tier, valid_to_ym ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS max_rate_avg_3,
        ROUND(PERCENT_RANK() OVER w_last_valid_to_date_month_by_2, 3) AS last_valid_to_percent_rank
    FROM tagged
    WINDOW w_first_valid_to_date_year AS (PARTITION BY YEAR(first_valid_to_date)), w_last_valid_to_date_month_by_2 AS (PARTITION BY DATE_TRUNC('MONTH', last_valid_to_date) ORDER BY last_valid_to, valid_from_day, rate_tier, valid_to_ym)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        valid_from_day,
        rate_tier,
        valid_to_ym,
        max_rate,
        avg_rate_distance,
        first_valid_to,
        first_valid_to_date,
        avg_rate_distance_vs_first_valid_to_date_year_av,
        max_rate_avg_3,
        last_valid_to_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', last_valid_to_date) ORDER BY last_valid_to_percent_rank), 3) AS last_valid_to_percent_rank_cume_dist,
        ROUND(AVG(max_rate_avg_3) OVER (PARTITION BY MONTH(first_valid_to_date), TO_DATE(last_valid_to)), 2) AS first_valid_to_date_month_last_valid_to_to_date_,
        max_rate - LAG(max_rate) OVER (PARTITION BY DATE_TRUNC('MONTH', last_valid_to_date) ORDER BY last_valid_to_percent_rank, valid_from_day, rate_tier, valid_to_ym) AS max_rate_change,
        SIZE(COLLECT_SET(rate_tier) OVER w_first_valid_to_date_month_la) AS distinct_rate_tier_in_first_valid_to_date_month_
    FROM ranked
    WINDOW w_first_valid_to_date_month_la AS (PARTITION BY MONTH(first_valid_to_date), TO_DATE(last_valid_to))
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE max_rate_change > (SELECT AVG(max_rate_change) FROM windowed)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT valid_from_day, rate_tier, max_rate_change
    FROM with_activity
) src
PIVOT (
    SUM(max_rate_change)
    FOR rate_tier IN ('LOW', 'MEDIUM')
)
