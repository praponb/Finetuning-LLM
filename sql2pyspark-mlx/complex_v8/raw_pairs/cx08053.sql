WITH filtered_tariffs AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        DATEDIFF(DATE '2025-06-30', valid_from) AS days_since_valid_from
    FROM tariff_windows
    WHERE (tariff = 'FLAT' OR MONTH(valid_from) IN (1, 3))
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', valid_from) ORDER BY days_since_valid_from DESC, id) AS days_since_valid_from_seq,
        MAX(days_since_valid_from) OVER (PARTITION BY tariff, DATE_TRUNC('MONTH', valid_from) ORDER BY valid_from, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_valid_from_max_3_prior,
        COUNT(*) OVER (PARTITION BY TO_DATE(valid_from), tariff) AS rows_in_valid_from_to_date_tariff,
        ROUND(days_since_valid_from - AVG(days_since_valid_from) OVER (PARTITION BY TO_DATE(valid_from), tariff), 2) AS days_since_valid_from_vs_valid_from_to_date_tari
    FROM filtered_tariffs
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN days_since_valid_from >= 420 THEN 'large' WHEN days_since_valid_from >= 315 THEN 'medium' ELSE 'small' END AS days_since_valid_from_tier,
        ABS(days_since_valid_from - 280) AS days_since_valid_from_distance
    FROM metrics
    WHERE rows_in_valid_from_to_date_tariff <= 18
)
-- Subtotals with ROLLUP
SELECT
    days_since_valid_from_seq,
    tariff,
    COUNT(DISTINCT tariff) AS distinct_tariffs,
    PERCENTILE_APPROX(rows_in_valid_from_to_date_tariff, 0.5) AS median_rows_in_valid_from_to_date_tariff,
    ROUND(100 * COUNT(CASE WHEN tariff IN ('TOU', 'EV') THEN 1 END) / COUNT(*), 1) AS pct_tariff_in_tou_ev
FROM derived
GROUP BY ROLLUP(days_since_valid_from_seq, tariff)
ORDER BY days_since_valid_from_seq NULLS LAST, tariff NULLS LAST
