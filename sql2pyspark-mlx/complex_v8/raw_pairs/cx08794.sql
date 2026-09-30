WITH cleaned AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        MONTH(valid_from) AS valid_from_month
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-01-25 21:00:00'
      AND tariff IN ('EV', 'FLAT')
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        valid_from_month,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from)) / 60, 1) AS minutes_valid_from_to_valid_to,
        QUARTER(valid_to) AS valid_to_quarter
    FROM cleaned
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        valid_to,
        minutes_valid_from_to_valid_to,
        valid_to_quarter,
        SUM(rate) OVER (PARTITION BY valid_from_month ORDER BY valid_from_month DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rate_sum_6_prior,
        NTH_VALUE(valid_to, 2) OVER (ORDER BY minutes_valid_from_to_valid_to ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to,
        FIRST_VALUE(rate) IGNORE NULLS OVER (PARTITION BY valid_from_month ORDER BY valid_from_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rate,
        MAX(rate) OVER (ORDER BY minutes_valid_from_to_valid_to ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_rate_so_far
    FROM derived
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(valid_to_quarter) OVER (PARTITION BY valid_to_quarter) AS valid_to_quarter_sum_valid_to_quarter,
        RANK() OVER w_second_valid_to_month_valid AS rate_sum_6_prior_rank,
        COUNT(*) OVER (PARTITION BY valid_to_quarter) AS rows_in_valid_to_quarter
    FROM windowed
    WINDOW w_second_valid_to_month_valid AS (PARTITION BY DATE_TRUNC('MONTH', second_valid_to), valid_to_quarter ORDER BY rate_sum_6_prior, id)
),
leaders AS (
    -- Top 3 per group
    SELECT
        id,
        valid_to,
        minutes_valid_from_to_valid_to,
        valid_to_quarter,
        rate_sum_6_prior,
        second_valid_to,
        first_known_rate,
        max_rate_so_far,
        valid_to_quarter_sum_valid_to_quarter,
        rows_in_valid_to_quarter
    FROM metrics
    WHERE rate_sum_6_prior_rank <= 3
)
-- Final projection
SELECT
    id,
    valid_to,
    minutes_valid_from_to_valid_to,
    valid_to_quarter,
    second_valid_to,
    first_known_rate,
    valid_to_quarter_sum_valid_to_quarter,
    rows_in_valid_to_quarter
FROM leaders
ORDER BY first_known_rate, valid_to_quarter DESC
