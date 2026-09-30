WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM tariff_windows
    WHERE tariff IN ('FLAT', 'TOU')
),
rollup_base AS (
    -- Group-level totals
    SELECT
        TO_DATE(valid_from) AS valid_from_day,
        YEAR(valid_to) AS valid_to_year,
        MIN(valid_from) AS first_valid_from,
        ROUND(STDDEV(rate), 2) AS stddev_rate,
        MIN(rate) AS min_rate
    FROM scoped
    GROUP BY TO_DATE(valid_from), YEAR(valid_to)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(min_rate / NULLIF(valid_to_year, 0), 3) AS min_rate_per_valid_to_year,
        COALESCE(valid_to_year, 0) AS valid_to_year_filled
    FROM rollup_base
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY valid_to_year_filled ORDER BY valid_to_year_filled DESC, valid_from_day, valid_to_year), 3) AS valid_to_year_filled_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY valid_to_year ORDER BY valid_to_year_filled DESC, valid_from_day, valid_to_year) AS rn,
        ROUND(min_rate / NULLIF(SUM(min_rate) OVER w_valid_to_year_filled, 0), 4) AS min_rate_share_of_valid_to_year_filled,
        LAG(min_rate_per_valid_to_year, 2) OVER (PARTITION BY valid_to_year_filled ORDER BY valid_to_year_filled DESC, valid_from_day, valid_to_year) AS prev2_min_rate_per_valid_to_year,
        FIRST_VALUE(valid_from_day) IGNORE NULLS OVER (ORDER BY valid_to_year_filled ASC, valid_from_day, valid_to_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_from_day
    FROM derived
    WINDOW w_valid_to_year_filled AS (PARTITION BY valid_to_year_filled)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        HOUR(first_valid_from) AS first_valid_from_hour,
        DATE_TRUNC('YEAR', valid_from_day) AS valid_from_day_year
    FROM sequenced
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from_day_year)) AS rows_in_valid_from_day_year_month,
        RANK() OVER (ORDER BY stddev_rate DESC, valid_from_day, valid_to_year) AS stddev_rate_rank
    FROM scored
)
-- Final projection
SELECT
    valid_from_day,
    first_valid_from,
    stddev_rate,
    min_rate_per_valid_to_year,
    valid_to_year_filled,
    valid_to_year_filled_cume_dist,
    rn,
    min_rate_share_of_valid_to_year_filled,
    first_known_valid_from_day,
    first_valid_from_hour,
    valid_from_day_year,
    rows_in_valid_from_day_year_month,
    stddev_rate_rank,
    first_valid_from + INTERVAL 30 MINUTES AS first_valid_from_plus_30m,
    ABS(stddev_rate_rank - 4) AS stddev_rate_rank_distance
FROM ranked
ORDER BY rn NULLS LAST, first_valid_from DESC NULLS FIRST
