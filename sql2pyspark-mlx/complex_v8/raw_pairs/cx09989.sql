WITH base_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE HOUR(valid_from) BETWEEN 9 AND 15
      AND tariff <> 'EV'
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM base_tariffs
    WHERE rate > (SELECT MIN(rate) FROM base_tariffs)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        LAG(valid_to) OVER w_tariff_by_rate AS prev_valid_to,
        SUM(rate) OVER (PARTITION BY tariff) AS tariff_sum_rate,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(valid_from), tariff ORDER BY valid_from DESC, id) AS valid_from_dense_rank,
        LAG(rate, 3, 0) OVER (PARTITION BY TO_DATE(valid_from), tariff ORDER BY valid_from DESC, id) AS prev3_rate,
        FIRST_VALUE(valid_to) OVER (PARTITION BY TO_DATE(valid_from), tariff ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_valid_to
    FROM qualified
    WINDOW w_tariff_by_rate AS (PARTITION BY tariff ORDER BY rate DESC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(first_valid_to, 'yyyy-MM-dd') AS first_valid_to_label,
        UPPER(TRIM(tariff)) AS tariff_clean,
        ROUND(tariff_sum_rate / NULLIF(rate, 0), 3) AS tariff_sum_rate_per_rate
    FROM with_trends
    WHERE prev_valid_to >= TIMESTAMP '2025-01-25 21:00:00'
),
with_trends_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        rate,
        prev_valid_to,
        valid_from_dense_rank,
        first_valid_to,
        first_valid_to_label,
        tariff_clean,
        tariff_sum_rate_per_rate,
        ROUND(prev3_rate / NULLIF(SUM(prev3_rate) OVER w_tariff_valid_from_dense_rank, 0), 4) AS prev3_rate_share_of_tariff_valid_from_dense_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(first_valid_to) ORDER BY first_valid_to ASC, id) AS first_valid_to_dense_rank,
        DENSE_RANK() OVER (PARTITION BY tariff, YEAR(first_valid_to) ORDER BY first_valid_to) AS first_valid_to_dense_rank_2,
        COUNT(*) OVER (PARTITION BY tariff, valid_from_dense_rank) AS rows_in_tariff_valid_from_dense_rank
    FROM annotated
    WINDOW w_tariff_valid_from_dense_rank AS (PARTITION BY tariff, valid_from_dense_rank)
)
SELECT
    id,
    rate,
    prev_valid_to,
    valid_from_dense_rank,
    tariff_clean,
    prev3_rate_share_of_tariff_valid_from_dense_rank,
    first_valid_to_dense_rank,
    first_valid_to_dense_rank_2,
    rows_in_tariff_valid_from_dense_rank,
    ABS(tariff_sum_rate_per_rate - 5.0) AS tariff_sum_rate_per_rate_distance,
    LENGTH(TRIM(first_valid_to_label)) AS first_valid_to_label_len
FROM with_trends_2
ORDER BY first_valid_to_dense_rank DESC
