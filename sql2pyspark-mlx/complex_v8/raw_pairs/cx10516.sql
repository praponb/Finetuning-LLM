WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_to + INTERVAL 7 DAYS AS valid_to_plus_7d,
        LOWER(TRIM(tariff)) AS tariff_clean,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM tariff_windows
    WHERE rate < 5.9
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE rate > (SELECT AVG(rate) FROM prepared)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE rate > (SELECT AVG(rate) FROM qualified)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        rate,
        tariff_clean,
        LEAD(valid_from) OVER (PARTITION BY MONTH(valid_to_plus_7d) ORDER BY valid_to NULLS LAST, id) AS next_valid_from,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff,
        ROW_NUMBER() OVER (PARTITION BY MONTH(valid_to_plus_7d) ORDER BY valid_to NULLS LAST, id) AS valid_to_seq
    FROM without_matches
)
SELECT
    id,
    rate,
    tariff_clean,
    rows_in_tariff,
    valid_to_seq
FROM sequenced
WHERE valid_to_seq >= 4
ORDER BY id, rows_in_tariff
