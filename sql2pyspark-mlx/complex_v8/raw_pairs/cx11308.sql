WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        DATEDIFF(DATE '2025-12-31', commissioned_on) AS days_since_commissioned_on
    FROM sites
    WHERE zone = 'Z3'
      AND tariff = 'TOU'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_commissioned_on - AVG(days_since_commissioned_on) OVER (PARTITION BY YEAR(commissioned_on), tariff), 2) AS days_since_commissioned_on_vs_commissioned_on_ye,
        ROUND(AVG(days_since_commissioned_on) OVER (PARTITION BY YEAR(commissioned_on), tariff), 2) AS commissioned_on_year_tariff_avg_days_since_commi,
        SUM(days_since_commissioned_on) OVER (PARTITION BY zone, tariff ORDER BY commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_commissioned_on,
        RANK() OVER (ORDER BY commissioned_on DESC, id) AS commissioned_on_rank,
        NTILE(10) OVER w_tariff_commissioned_on_month AS commissioned_on_decile,
        days_since_commissioned_on - LAG(days_since_commissioned_on) OVER w_zone_by_days_since_commissio AS days_since_commissioned_on_change,
        LEAD(days_since_commissioned_on) OVER (PARTITION BY zone ORDER BY days_since_commissioned_on NULLS LAST, id) AS next_days_since_commissioned_on,
        MAX(days_since_commissioned_on) OVER (PARTITION BY YEAR(commissioned_on), tariff) AS commissioned_on_year_tariff_max_days_since_commi,
        LAG(commissioned_on) OVER (PARTITION BY zone ORDER BY days_since_commissioned_on NULLS LAST, id) AS prev_commissioned_on
    FROM scoped
    WINDOW w_tariff_commissioned_on_month AS (PARTITION BY tariff, MONTH(commissioned_on) ORDER BY commissioned_on, id), w_zone_by_days_since_commissio AS (PARTITION BY zone ORDER BY days_since_commissioned_on NULLS LAST, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        days_since_commissioned_on_vs_commissioned_on_ye,
        commissioned_on_rank,
        days_since_commissioned_on_change,
        commissioned_on_year_tariff_max_days_since_commi,
        SIZE(COLLECT_SET(tariff) OVER (PARTITION BY zone)) AS distinct_tariff_in_zone,
        ROUND(days_since_commissioned_on / NULLIF(SUM(days_since_commissioned_on) OVER (PARTITION BY zone), 0), 4) AS days_since_commissioned_on_share_of_zone,
        FIRST_VALUE(prev_commissioned_on) IGNORE NULLS OVER (PARTITION BY commissioned_on_decile, commissioned_on_rank ORDER BY prev_commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_commissioned_on,
        ROUND(PERCENT_RANK() OVER w_all_by_days_since_commission, 3) AS days_since_commissioned_on_vs_commissioned_on_ye_2,
        ROUND(PERCENT_RANK() OVER w_all_by_commissioned_on_decil, 3) AS commissioned_on_decile_percent_rank,
        COUNT(*) OVER (PARTITION BY commissioned_on_decile, commissioned_on_rank ORDER BY prev_commissioned_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        FIRST_VALUE(commissioned_on) OVER (ORDER BY commissioned_on_decile DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_commissioned_on
    FROM with_trends
    WINDOW w_all_by_days_since_commission AS (ORDER BY days_since_commissioned_on_vs_commissioned_on_ye ASC, id), w_all_by_commissioned_on_decil AS (ORDER BY commissioned_on_decile DESC NULLS FIRST, id)
)
SELECT
    id,
    site_name,
    zone,
    tariff,
    days_since_commissioned_on_vs_commissioned_on_ye,
    commissioned_on_rank,
    days_since_commissioned_on_change,
    commissioned_on_year_tariff_max_days_since_commi,
    distinct_tariff_in_zone,
    days_since_commissioned_on_share_of_zone,
    first_known_prev_commissioned_on,
    days_since_commissioned_on_vs_commissioned_on_ye_2,
    commissioned_on_decile_percent_rank,
    running_count,
    first_commissioned_on,
    DATEDIFF(commissioned_on, first_commissioned_on) AS days_first_commissioned_on_to_commissioned_on,
    DATEDIFF(DATE '2025-06-30', first_commissioned_on) AS days_since_first_commissioned_on
FROM with_windows
