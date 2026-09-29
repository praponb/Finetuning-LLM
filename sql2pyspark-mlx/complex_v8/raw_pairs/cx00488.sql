WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE tariff NOT IN ('FLAT', 'TOU')
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        DATEDIFF(DATE '2026-01-01', valid_to) AS days_since_valid_to,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown,
        CASE WHEN tariff IN ('EV', 'FLAT') THEN 'group_a' WHEN tariff = 'TOU' THEN 'group_b' ELSE 'other' END AS tariff_group
    FROM cleaned
    WHERE HOUR(valid_to) BETWEEN 9 AND 12
),
unique_rows AS (
    -- Keep the latest row per tariff_or_unknown, tariff_group
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        days_since_valid_to,
        tariff_or_unknown,
        tariff_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff_or_unknown, tariff_group ORDER BY days_since_valid_to DESC, id DESC) AS row_num
        FROM annotated
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER w_valid_to_month_by_valid_from AS valid_from_decile,
        ROUND(AVG(rate) OVER w_tariff_group, 2) AS tariff_group_avg_rate
    FROM unique_rows
    WINDOW w_valid_to_month_by_valid_from AS (PARTITION BY DATE_TRUNC('MONTH', valid_to) ORDER BY valid_from DESC, id), w_tariff_group AS (PARTITION BY tariff_group)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        valid_from_decile,
        ROUND(STDDEV(valid_from_decile), 2) AS stddev_valid_from_decile,
        ROUND(AVG(valid_from_decile), 2) AS avg_valid_from_decile
    FROM with_trends
    WHERE days_since_valid_to >= 140
    GROUP BY 1
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE valid_from_decile > (SELECT AVG(valid_from_decile) FROM per_group)
),
labeled AS (
    -- Add calculated columns
    SELECT
        valid_from_decile,
        stddev_valid_from_decile,
        avg_valid_from_decile,
        ROUND(avg_valid_from_decile * 1.07, 2) AS avg_valid_from_decile_adj,
        COALESCE(valid_from_decile, 0) AS valid_from_decile_filled,
        ABS(valid_from_decile - 4) AS valid_from_decile_distance
    FROM filtered
    WHERE valid_from_decile > 5
)
SELECT
    valid_from_decile,
    stddev_valid_from_decile,
    avg_valid_from_decile,
    avg_valid_from_decile_adj,
    valid_from_decile_filled,
    valid_from_decile_distance
FROM labeled
ORDER BY valid_from_decile_filled DESC
