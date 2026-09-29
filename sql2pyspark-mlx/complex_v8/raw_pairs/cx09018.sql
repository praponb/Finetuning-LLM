WITH prepared_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CASE WHEN rate >= 5.9 THEN 'large' WHEN rate >= 3.7 THEN 'medium' ELSE 'small' END AS rate_tier,
        LOWER(TRIM(tariff)) AS tariff_clean
    FROM tariff_windows
    WHERE (TO_DATE(valid_to) < DATE '2025-03-12' OR valid_from < TIMESTAMP '2025-03-09 04:00:00')
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_tariffs
    WHERE rate > (SELECT AVG(rate) FROM prepared_tariffs)
)
SELECT *
FROM (
    SELECT valid_to, tariff, rate
    FROM without_matches
) src
PIVOT (
    SUM(rate)
    FOR tariff IN ('TOU', 'FLAT', 'EV')
)
ORDER BY valid_to
