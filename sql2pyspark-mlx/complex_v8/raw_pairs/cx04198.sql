WITH prepared_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_to,
        rate,
        DATE_TRUNC('QUARTER', valid_to) AS valid_to_quarter,
        SUBSTRING(TRIM(tariff), 1, 3) AS tariff_prefix
    FROM tariff_windows
    WHERE rate >= 3.0
      AND valid_from < TIMESTAMP '2025-03-09 05:00:00'
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared_tariffs
    WHERE rate < (SELECT MAX(rate) FROM prepared_tariffs)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(valid_to_quarter) - UNIX_TIMESTAMP(valid_to)) / 60, 1) AS minutes_valid_to_to_valid_to_quarter,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_to_quarter)) / 3600, 1) AS hours_valid_to_quarter_to_valid_to,
        CONCAT(tariff_prefix, '#', CAST(id AS STRING)) AS tariff_prefix_ref
    FROM screened
    WHERE MONTH(valid_to_quarter) IN (1, 2)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, amount
FROM (
    SELECT id, rate, hours_valid_to_quarter_to_valid_to
    FROM labeled
) src
UNPIVOT (
    amount FOR metric IN (rate AS rate_v, hours_valid_to_quarter_to_valid_to AS m_hours_valid_to_quarter_to_valid_to)
) unp
