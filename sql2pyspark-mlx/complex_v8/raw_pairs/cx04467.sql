WITH cleaned_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        DATE_FORMAT(valid_to, 'yyyy-MM') AS valid_to_label
    FROM tariff_windows
    WHERE (valid_to >= TIMESTAMP '2025-02-15 04:00:00' OR rate >= 4.2)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(valid_from) AS valid_from_year,
        YEAR(valid_to) AS valid_to_year,
        tariff,
        COUNT(*) AS n_rows,
        MIN(valid_to) AS first_valid_to,
        SUM(CASE WHEN tariff IN ('FLAT', 'EV') THEN 1 ELSE 0 END) AS n_tariff_in_flat_ev,
        SUM(CASE WHEN tariff IS NOT NULL THEN 1 ELSE 0 END) AS n_tariff_is_not_null
    FROM cleaned_tariffs
    GROUP BY YEAR(valid_from), YEAR(valid_to), tariff
),
enriched AS (
    -- Add calculated columns
    SELECT
        valid_from_year,
        valid_to_year,
        tariff,
        n_rows,
        first_valid_to,
        MONTH(first_valid_to) AS first_valid_to_month,
        ABS(n_tariff_is_not_null - 4) AS n_tariff_is_not_null_distance
    FROM stats
)
-- Subtotals with GROUPING SETS
SELECT
    first_valid_to_month,
    valid_from_year,
    n_rows,
    GROUPING(first_valid_to_month) AS is_total_first_valid_to_month,
    ROUND(STDDEV(n_rows), 2) AS stddev_n_rows,
    COUNT(*) FILTER (WHERE tariff IS NOT NULL) AS n_tariff_is_not_null
FROM enriched
GROUP BY GROUPING SETS ((first_valid_to_month, valid_from_year, n_rows), (first_valid_to_month), (valid_from_year))
