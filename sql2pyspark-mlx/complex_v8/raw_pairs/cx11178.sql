WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        INITCAP(TRIM(tariff)) AS tariff_clean
    FROM tariff_windows
    WHERE tariff IN ('FLAT', 'TOU')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (ORDER BY valid_from) AS valid_from_rank,
        RANK() OVER (ORDER BY valid_from, id) AS valid_from_rank_2,
        SUM(rate) OVER w_valid_from_to_date_tariff AS valid_from_to_date_tariff_sum_rate
    FROM cleaned
    WINDOW w_valid_from_to_date_tariff AS (PARTITION BY TO_DATE(valid_from), tariff)
),
tagged AS (
    -- Label and score rows
    SELECT *, CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM ranked
    WHERE valid_from_rank <= 9
)
-- Final projection
SELECT
    id,
    tariff,
    rate,
    tariff_clean,
    valid_from_rank,
    valid_from_rank_2,
    valid_from_to_date_tariff_sum_rate,
    tariff_ref,
    DATE_FORMAT(valid_from, 'yyyy-MM-dd') AS valid_from_label
FROM tagged
ORDER BY tariff, tariff_ref
