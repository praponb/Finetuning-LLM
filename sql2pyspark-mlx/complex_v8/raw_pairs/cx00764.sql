WITH recent_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        DATE_TRUNC('DAY', valid_to) AS valid_to_day
    FROM tariff_windows
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent_tariffs
    WHERE rate > (SELECT AVG(rate) FROM recent_tariffs)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        valid_from,
        valid_to,
        valid_to_day,
        LAG(rate) OVER w_valid_to_month_by_valid_from AS prev_rate,
        MAX(rate) OVER (PARTITION BY DATE_TRUNC('MONTH', valid_to) ORDER BY valid_from DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rate_max_3_prior,
        SUM(rate) OVER w_valid_from_to_date AS valid_from_to_date_sum_rate
    FROM qualified
    WINDOW w_valid_to_month_by_valid_from AS (PARTITION BY DATE_TRUNC('MONTH', valid_to) ORDER BY valid_from DESC, id), w_valid_from_to_date AS (PARTITION BY TO_DATE(valid_from))
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM with_trends
    WHERE rate_max_3_prior > (SELECT MIN(rate_max_3_prior) FROM with_trends)
)
SELECT
    id,
    valid_to,
    prev_rate,
    rate_max_3_prior,
    valid_from_to_date_sum_rate
FROM without_matches
ORDER BY rate_max_3_prior DESC NULLS FIRST
