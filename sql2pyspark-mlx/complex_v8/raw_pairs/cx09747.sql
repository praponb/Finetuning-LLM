WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE MONTH(valid_from) IN (1, 2)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM recent
    WHERE rate > (SELECT AVG(rate) FROM recent)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(valid_from) AS valid_from_year,
        YEAR(valid_to) AS valid_to_year,
        MIN(valid_from) AS first_valid_from,
        MIN(valid_to) AS first_valid_to
    FROM filtered
    GROUP BY YEAR(valid_from), YEAR(valid_to)
)
-- Final projection
SELECT
    valid_from_year,
    valid_to_year,
    first_valid_to,
    LEAST(valid_from_year, valid_to_year) AS least_valid_from_year_valid_to_year,
    DATE_TRUNC('YEAR', first_valid_to) AS first_valid_to_year
FROM summary
WHERE HOUR(first_valid_to) BETWEEN 7 AND 10
ORDER BY least_valid_from_year_valid_to_year NULLS LAST, valid_to_year DESC
