WITH recent_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        SUBSTRING(TRIM(tariff), 1, 4) AS tariff_prefix,
        UPPER(TRIM(tariff)) AS tariff_clean
    FROM tariff_windows
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(rate - AVG(rate) OVER (PARTITION BY MONTH(valid_from)), 2) AS rate_vs_valid_from_month_avg,
        COUNT(*) OVER w_valid_to_month AS rows_in_valid_to_month,
        NTILE(4) OVER w_valid_to_to_date_tariff_by_v AS valid_from_quartile,
        SUM(rate) OVER (PARTITION BY MONTH(valid_from)) AS valid_from_month_sum_rate,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER (PARTITION BY MONTH(valid_from) ORDER BY rate, id))) / 60, 1) AS minutes_since_prev_valid_from,
        LEAD(valid_to, 2) OVER (PARTITION BY tariff ORDER BY valid_from, id) AS next2_valid_to,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY MONTH(valid_from)), 0), 4) AS rate_share_of_valid_from_month,
        FIRST_VALUE(valid_to) OVER (ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_valid_to,
        ROUND(AVG(rate) OVER (PARTITION BY YEAR(valid_from), tariff), 2) AS valid_from_year_tariff_avg_rate,
        NTILE(3) OVER w_all_by_valid_from AS valid_from_tercile,
        NTH_VALUE(valid_to, 2) OVER (ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(LAG(valid_to) OVER (PARTITION BY MONTH(valid_from) ORDER BY rate, id))) / 60, 1) AS minutes_since_prev_valid_to,
        ROUND(AVG(rate) OVER (PARTITION BY MONTH(valid_from)), 2) AS valid_from_month_avg_rate,
        SUM(rate) OVER w_valid_to_month AS valid_to_month_sum_rate
    FROM recent_tariffs
    WINDOW w_valid_to_month AS (PARTITION BY MONTH(valid_to)), w_valid_to_to_date_tariff_by_v AS (PARTITION BY TO_DATE(valid_to), tariff ORDER BY valid_from DESC NULLS FIRST, id), w_all_by_valid_from AS (ORDER BY valid_from, id)
),
tagged AS (
    -- Label and score rows
    SELECT *, MONTH(next2_valid_to) AS next2_valid_to_month
    FROM ranked
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        tariff_prefix,
        rate_vs_valid_from_month_avg,
        rows_in_valid_to_month,
        valid_from_quartile,
        valid_from_month_sum_rate,
        minutes_since_prev_valid_from,
        rate_share_of_valid_from_month,
        valid_from_tercile,
        second_valid_to,
        valid_from_month_avg_rate,
        valid_to_month_sum_rate,
        FIRST_VALUE(second_valid_to) OVER (ORDER BY valid_from_month_sum_rate, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_second_valid_to,
        MIN(rows_in_valid_to_month) OVER w_next2_valid_to_month_valid_f AS next2_valid_to_month_valid_from_tercile_min_rows,
        ROUND(CUME_DIST() OVER (ORDER BY valid_from_month_sum_rate), 3) AS valid_from_month_sum_rate_cume_dist
    FROM tagged
    WINDOW w_next2_valid_to_month_valid_f AS (PARTITION BY next2_valid_to_month, valid_from_tercile)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(valid_from) AS valid_from_year,
        YEAR(first_second_valid_to) AS first_second_valid_to_year,
        MIN(valid_from) AS first_valid_from,
        ROUND(SUM(valid_from_quartile) / NULLIF(SUM(valid_to_month_sum_rate), 0), 3) AS valid_from_quartile_to_valid_to_month_sum_rate_r,
        ROUND(STDDEV(rate_vs_valid_from_month_avg), 2) AS stddev_rate_vs_valid_from_month_avg,
        ROUND(STDDEV(next2_valid_to_month_valid_from_tercile_min_rows), 2) AS stddev_next2_valid_to_month_valid_from_tercile_m
    FROM sequenced
    GROUP BY 1, 2
)
-- Final projection
SELECT
    valid_from_year,
    first_second_valid_to_year,
    valid_from_quartile_to_valid_to_month_sum_rate_r,
    stddev_rate_vs_valid_from_month_avg,
    stddev_next2_valid_to_month_valid_from_tercile_m,
    YEAR(first_valid_from) AS first_valid_from_year,
    YEAR(first_valid_from) AS first_valid_from_year_2
FROM stats
ORDER BY first_valid_from_year_2 NULLS LAST, valid_from_year, first_second_valid_to_year
LIMIT 20
