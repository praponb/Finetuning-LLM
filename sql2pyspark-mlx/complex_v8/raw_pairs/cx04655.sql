WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        ABS(rate - 5.2) AS rate_distance
    FROM tariff_windows
    WHERE tariff IN ('FLAT', 'EV')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        valid_to,
        rate,
        rate_distance,
        ROUND(rate_distance / NULLIF(SUM(rate_distance) OVER (PARTITION BY TO_DATE(valid_to)), 0), 4) AS rate_distance_share_of_valid_to_to_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY valid_to DESC, id), 3) AS valid_to_percent_rank,
        FIRST_VALUE(valid_to) OVER (ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_valid_to,
        ROUND((UNIX_TIMESTAMP(valid_from) - UNIX_TIMESTAMP(LAG(valid_from) OVER (PARTITION BY YEAR(valid_from) ORDER BY rate_distance DESC, id))) / 60, 1) AS minutes_since_prev_valid_from,
        ROW_NUMBER() OVER (PARTITION BY YEAR(valid_from) ORDER BY rate_distance DESC, id) AS rn,
        NTH_VALUE(rate_distance, 2) OVER (ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rate_distance,
        ROUND(PERCENT_RANK() OVER (ORDER BY rate DESC, id), 3) AS rate_percent_rank,
        MAX(rate) OVER (ORDER BY rate_distance NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rate_max_4,
        MAX(rate_distance) OVER (PARTITION BY TO_DATE(valid_to)) AS valid_to_to_date_max_rate_distance,
        NTH_VALUE(valid_to, 2) OVER (ORDER BY rate DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to,
        RANK() OVER (PARTITION BY YEAR(valid_from) ORDER BY rate_distance DESC) AS rate_distance_rank,
        COUNT(*) OVER (ORDER BY rate_distance NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(rate_distance) OVER (ORDER BY valid_to DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rate_distance_max_2_prior,
        SUM(rate) OVER (ORDER BY rate DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rate_sum_3,
        ROUND(rate_distance - AVG(rate_distance) OVER (PARTITION BY TO_DATE(valid_to)), 2) AS rate_distance_vs_valid_to_to_date_avg,
        FIRST_VALUE(valid_to) IGNORE NULLS OVER (PARTITION BY YEAR(valid_from) ORDER BY rate_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_to
    FROM filtered
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        valid_to,
        rate,
        rate_distance,
        rate_distance_share_of_valid_to_to_date,
        valid_to_percent_rank,
        first_valid_to,
        minutes_since_prev_valid_from,
        second_rate_distance,
        rate_percent_rank,
        rate_max_4,
        valid_to_to_date_max_rate_distance,
        second_valid_to,
        rate_distance_rank,
        running_count,
        rate_distance_max_2_prior,
        rate_sum_3,
        rate_distance_vs_valid_to_to_date_avg,
        first_known_valid_to
    FROM sequenced
    WHERE rn <= 5
)
SELECT
    id,
    valid_to,
    rate,
    valid_to_percent_rank,
    first_valid_to,
    minutes_since_prev_valid_from,
    second_rate_distance,
    rate_percent_rank,
    rate_max_4,
    second_valid_to,
    running_count,
    rate_sum_3,
    first_known_valid_to
FROM top_per_group
ORDER BY second_valid_to DESC
