WITH cleaned AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM tariff_windows
    WHERE (valid_from >= TIMESTAMP '2025-02-15 04:00:00' OR tariff IS NOT NULL)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        valid_to,
        rate,
        tariff_len,
        ROUND(rate - AVG(rate) OVER (PARTITION BY tariff_len), 2) AS rate_vs_tariff_len_avg,
        MAX(tariff_len) OVER (PARTITION BY tariff_len) AS tariff_len_max_tariff_len
    FROM cleaned
),
scored AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(valid_to, 'MMM yyyy') AS valid_to_label
    FROM windowed
)
SELECT
    id,
    rate,
    tariff_len,
    rate_vs_tariff_len_avg,
    tariff_len_max_tariff_len,
    valid_to_label,
    ABS(rate_vs_tariff_len_avg - -1.6) AS rate_vs_tariff_len_avg_distance
FROM scored
