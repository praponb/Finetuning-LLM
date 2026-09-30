WITH scoped_sites AS (
    -- Scope the sites rows
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on
    FROM sites
    WHERE installed_kw >= 93.3
      AND tariff = 'EV'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', commissioned_on) AS commissioned_on_month,
        COUNT(DISTINCT zone) AS distinct_zones,
        COUNT(*) AS cnt,
        MAX(commissioned_on) AS last_commissioned_on
    FROM scoped_sites
    GROUP BY DATE_TRUNC('MONTH', commissioned_on)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        commissioned_on_month,
        distinct_zones,
        cnt,
        last_commissioned_on,
        ROUND(PERCENT_RANK() OVER w_all_by_cnt, 3) AS cnt_percent_rank,
        MAX(distinct_zones) OVER (PARTITION BY cnt, YEAR(commissioned_on_month)) AS cnt_commissioned_on_month_year_max_distinct_zone,
        MAX(distinct_zones) OVER (ORDER BY cnt DESC, commissioned_on_month ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS distinct_zones_max_3_prior,
        ROUND(AVG(cnt) OVER (PARTITION BY distinct_zones, MONTH(last_commissioned_on) ORDER BY last_commissioned_on ASC, commissioned_on_month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS cnt_avg_1_prior
    FROM totals
    WINDOW w_all_by_cnt AS (ORDER BY cnt DESC, commissioned_on_month)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        commissioned_on_month + INTERVAL 15 MINUTES AS commissioned_on_month_plus_15m
    FROM with_trends
    WHERE last_commissioned_on < DATE '2025-05-10'
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_zones,
    cnt,
    MAX(commissioned_on_month) AS last_commissioned_on_month,
    ROUND(SUM(cnt_avg_1_prior), 2) AS total_cnt_avg_1_prior
FROM derived
GROUP BY GROUPING SETS ((distinct_zones, cnt), (distinct_zones), ())
ORDER BY distinct_zones NULLS LAST, cnt NULLS LAST
