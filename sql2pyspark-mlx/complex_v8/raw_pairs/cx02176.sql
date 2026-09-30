WITH prepared_tariffs AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE rate >= 3.0
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        LENGTH(TRIM(tariff)) AS tariff_len,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref_2
    FROM prepared_tariffs
),
annotated_2 AS (
    -- Derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        tariff_len,
        tariff_ref,
        tariff_ref_2,
        TO_DATE(valid_from) AS valid_from_date
    FROM annotated
),
annotated_3 AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(valid_from, 'MMM yyyy') AS valid_from_label
    FROM annotated_2
)
SELECT
    COALESCE(tariff, 'ALL') AS tariff,
    tariff_len,
    GROUPING(tariff) AS is_total_tariff,
    COUNT(*) AS cnt
FROM annotated_3
GROUP BY GROUPING SETS ((tariff, tariff_len), (tariff), (tariff_len), ())
