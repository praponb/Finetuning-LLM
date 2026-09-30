WITH recent_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        LENGTH(TRIM(tariff)) AS tariff_len,
        ABS(rate - 4.2) AS rate_distance,
        TO_DATE(valid_from) AS valid_from_date
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-02-18 04:00:00'
      AND tariff IN ('TOU', 'FLAT')
),
tariff_stats AS (
    -- Avg tariff_len per tariff
    SELECT tariff, AVG(tariff_len) AS avg_tariff_len
    FROM recent_tariffs
    GROUP BY tariff
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_tariff_len,
        ROUND(s.tariff_len - g.avg_tariff_len, 2) AS tariff_len_minus_avg
    FROM recent_tariffs s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.tariff_len > g.avg_tariff_len
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM vs_group
    WHERE avg_tariff_len > (SELECT AVG(avg_tariff_len) FROM vs_group)
)
SELECT
    id,
    rate,
    tariff_len,
    avg_tariff_len
FROM with_activity
ORDER BY tariff_len, avg_tariff_len DESC NULLS FIRST
