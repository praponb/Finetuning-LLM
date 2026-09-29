WITH prepared AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to
    FROM tariff_windows
    WHERE MONTH(valid_to) IN (1, 2)
      AND tariff = 'EV'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(valid_from) IGNORE NULLS OVER (PARTITION BY MONTH(valid_from), TO_DATE(valid_to) ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_valid_from,
        ROW_NUMBER() OVER (PARTITION BY tariff, TO_DATE(valid_to) ORDER BY valid_from, id) AS valid_from_seq,
        LAG(valid_from) OVER (PARTITION BY tariff, TO_DATE(valid_to) ORDER BY valid_from, id) AS prev_valid_from
    FROM prepared
),
tariff_stats AS (
    -- Avg valid_from_seq per tariff
    SELECT tariff, AVG(valid_from_seq) AS avg_valid_from_seq
    FROM with_windows
    GROUP BY tariff
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_valid_from_seq,
        ROUND(s.valid_from_seq - g.avg_valid_from_seq, 2) AS valid_from_seq_minus_avg
    FROM with_windows s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.valid_from_seq > g.avg_valid_from_seq
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(valid_from) AS valid_from_day,
        COUNT(*) FILTER (WHERE tariff = 'TOU') AS n_tariff_tou,
        MAX(valid_from_seq_minus_avg) AS max_valid_from_seq_minus_avg,
        COUNT(*) AS row_count,
        ROUND(SUM(valid_from_seq_minus_avg) / NULLIF(SUM(avg_valid_from_seq), 0), 3) AS valid_from_seq_minus_avg_to_avg_valid_from_seq_r
    FROM outliers
    GROUP BY TO_DATE(valid_from)
    HAVING COUNT(*) >= 2
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE row_count > (SELECT AVG(row_count) FROM stats)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(row_count) OVER (PARTITION BY row_count, MONTH(valid_from_day)) AS row_count_valid_from_day_month_sum_row_count,
        DATEDIFF(valid_from_day, LAG(valid_from_day) OVER (PARTITION BY n_tariff_tou, YEAR(valid_from_day) ORDER BY n_tariff_tou ASC, valid_from_day)) AS days_since_prev_valid_from_day,
        FIRST_VALUE(max_valid_from_seq_minus_avg) IGNORE NULLS OVER (PARTITION BY n_tariff_tou, YEAR(valid_from_day) ORDER BY n_tariff_tou ASC, valid_from_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_valid_from_seq_minus_avg,
        COUNT(*) OVER w_row_count_valid_from_day_mon AS rows_in_row_count_valid_from_day_month,
        NTH_VALUE(n_tariff_tou, 2) OVER (PARTITION BY n_tariff_tou, YEAR(valid_from_day) ORDER BY n_tariff_tou ASC, valid_from_day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_tariff_tou
    FROM without_matches
    WINDOW w_row_count_valid_from_day_mon AS (PARTITION BY row_count, MONTH(valid_from_day))
)
-- Final projection
SELECT
    valid_from_day,
    n_tariff_tou,
    row_count,
    valid_from_seq_minus_avg_to_avg_valid_from_seq_r,
    row_count_valid_from_day_month_sum_row_count,
    days_since_prev_valid_from_day,
    first_known_max_valid_from_seq_minus_avg,
    rows_in_row_count_valid_from_day_month,
    ABS(second_n_tariff_tou - 6) AS second_n_tariff_tou_distance
FROM windowed
ORDER BY first_known_max_valid_from_seq_minus_avg DESC
