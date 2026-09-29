WITH base_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        TO_DATE(valid_from) AS valid_from_date
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-03-09 04:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from_date) ORDER BY valid_to DESC, id) AS valid_to_decile,
        FIRST_VALUE(valid_to) IGNORE NULLS OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from_date) ORDER BY valid_to DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_to,
        RANK() OVER w_tariff_valid_from_date_month_3 AS valid_to_rank,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(LAG(valid_to) OVER w_tariff_valid_from_date_month)) / 60, 1) AS minutes_since_prev_valid_to
    FROM base_tariffs
    WINDOW w_tariff_valid_from_date_month_3 AS (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from_date) ORDER BY valid_to DESC), w_tariff_valid_from_date_month AS (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from_date) ORDER BY valid_to DESC, id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        valid_to_decile,
        valid_to_rank,
        minutes_since_prev_valid_to,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        ABS(valid_to_rank - 7) AS valid_to_rank_distance,
        DATE_TRUNC('WEEK', valid_from_date) AS valid_from_date_week
    FROM sequenced
),
tagged AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('WEEK', valid_to) AS valid_to_week
    FROM scored
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        valid_from,
        valid_from_date_week,
        valid_to_week,
        ROUND(valid_to_rank_distance - AVG(valid_to_rank_distance) OVER w_valid_to_rank_valid_to_decil, 2) AS valid_to_rank_distance_vs_valid_to_rank_valid_to,
        LAST_VALUE(valid_to_rank) IGNORE NULLS OVER (PARTITION BY valid_to_decile, YEAR(valid_from_date_week) ORDER BY valid_to_decile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_to_rank,
        SUM(minutes_since_prev_valid_to) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from) ORDER BY valid_to_decile DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS minutes_since_prev_valid_to_sum_2_prior,
        LAG(valid_to_rank, 3, 0) OVER w_valid_from_month_by_valid_to_2 AS prev3_valid_to_rank
    FROM tagged
    WINDOW w_valid_to_rank_valid_to_decil AS (PARTITION BY valid_to_rank, valid_to_decile), w_valid_from_month_by_valid_to_2 AS (PARTITION BY DATE_TRUNC('MONTH', valid_from) ORDER BY valid_to_decile DESC, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY last_known_valid_to_rank, prev3_valid_to_rank ORDER BY last_known_valid_to_rank DESC, id) AS last_known_valid_to_rank_rank,
        ROUND(AVG(prev3_valid_to_rank) OVER (PARTITION BY last_known_valid_to_rank, prev3_valid_to_rank ORDER BY last_known_valid_to_rank DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS prev3_valid_to_rank_avg_3_prior,
        ROUND(last_known_valid_to_rank / NULLIF(SUM(last_known_valid_to_rank) OVER (PARTITION BY TO_DATE(valid_from_date_week), prev3_valid_to_rank), 0), 4) AS last_known_valid_to_rank_share_of_valid_from_dat,
        ROUND(last_known_valid_to_rank - AVG(last_known_valid_to_rank) OVER (PARTITION BY TO_DATE(valid_from_date_week), prev3_valid_to_rank), 2) AS last_known_valid_to_rank_vs_valid_from_date_week,
        MAX(last_known_valid_to_rank) OVER (PARTITION BY TO_DATE(valid_from_date_week), prev3_valid_to_rank) AS valid_from_date_week_to_date_prev3_valid_to_rank
    FROM with_trends
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, minutes_since_prev_valid_to_sum_2_prior, prev3_valid_to_rank_avg_3_prior, last_known_valid_to_rank_vs_valid_from_date_week
    FROM ranked
) src
UNPIVOT (
    value FOR field IN (minutes_since_prev_valid_to_sum_2_prior, prev3_valid_to_rank_avg_3_prior, last_known_valid_to_rank_vs_valid_from_date_week)
) unp
ORDER BY id, field
