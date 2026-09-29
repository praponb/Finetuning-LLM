WITH valid_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE rate <= 5.9
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY tariff), 0), 4) AS rate_share_of_tariff,
        RANK() OVER w_tariff_by_valid_to AS valid_to_rank,
        MAX(rate) OVER (PARTITION BY tariff ORDER BY valid_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_rate_so_far,
        MAX(rate) OVER (PARTITION BY tariff) AS tariff_max_rate,
        RANK() OVER w_tariff_by_valid_to_ties AS valid_to_rank_2,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff,
        LEAD(valid_from, 2) OVER w_tariff_by_valid_to AS next2_valid_from,
        ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_from, id) AS seq_no,
        SUM(rate) OVER (PARTITION BY tariff ORDER BY valid_to, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rate,
        ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_to, id) AS seq_no_2
    FROM valid_tariffs
    WINDOW w_tariff_by_valid_to AS (PARTITION BY tariff ORDER BY valid_to, id), w_tariff_by_valid_to_ties AS (PARTITION BY tariff ORDER BY valid_to)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        INITCAP(TRIM(tariff)) AS tariff_clean,
        valid_to + INTERVAL 1 DAYS AS valid_to_plus_1d,
        HOUR(next2_valid_from) AS next2_valid_from_hour
    FROM windowed
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        next2_valid_from_hour,
        COUNT(DISTINCT tariff) AS distinct_tariffs,
        ROUND(100 * COUNT(CASE WHEN tariff = 'FLAT' THEN 1 END) / COUNT(*), 1) AS pct_tariff_flat,
        COUNT(*) AS n_rows,
        MAX(valid_to) AS last_valid_to,
        ROUND(SUM(running_rate), 2) AS total_running_rate
    FROM tagged
    GROUP BY next2_valid_from_hour
)
-- Final projection
SELECT
    next2_valid_from_hour,
    distinct_tariffs,
    TO_DATE(last_valid_to) AS last_valid_to_date,
    LEAST(next2_valid_from_hour, distinct_tariffs) AS least_next2_valid_from_hour_distinct_tariffs
FROM stats
ORDER BY last_valid_to_date DESC, least_next2_valid_from_hour_distinct_tariffs DESC
