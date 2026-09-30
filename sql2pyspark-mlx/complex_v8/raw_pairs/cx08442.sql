WITH cleaned_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_to,
        rate
    FROM tariff_windows
    WHERE tariff IN ('FLAT', 'EV')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(rate) OVER (PARTITION BY tariff, TO_DATE(valid_to)) AS tariff_valid_to_to_date_sum_rate,
        MIN(rate) OVER (PARTITION BY MONTH(valid_to), tariff ORDER BY valid_to DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_rate_so_far,
        ROUND(AVG(rate) OVER (PARTITION BY tariff, TO_DATE(valid_to)), 2) AS tariff_valid_to_to_date_avg_rate,
        COUNT(*) OVER (PARTITION BY tariff, TO_DATE(valid_to)) AS rows_in_tariff_valid_to_to_date,
        NTILE(4) OVER (PARTITION BY MONTH(valid_to), tariff ORDER BY valid_to DESC, id) AS valid_to_quartile,
        MAX(rate) OVER w_tariff_valid_to_to_date AS tariff_valid_to_to_date_max_rate
    FROM cleaned_tariffs
    WINDOW w_tariff_valid_to_to_date AS (PARTITION BY tariff, TO_DATE(valid_to))
),
per_group AS (
    -- Group-level totals
    SELECT
        tariff,
        ROUND(SUM(tariff_valid_to_to_date_max_rate) / NULLIF(SUM(rows_in_tariff_valid_to_to_date), 0), 3) AS tariff_valid_to_to_date_max_rate_to_rows_in_tari,
        MIN(valid_to) AS first_valid_to
    FROM windowed
    GROUP BY 1
    HAVING AVG(rows_in_tariff_valid_to_to_date) > 4
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(first_valid_to, 'yyyy-MM-dd') AS first_valid_to_label,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM per_group
),
tariff_or_unknown_stats AS (
    -- Median tariff_valid_to_to_date_max_rate_to_rows_in_tari per tariff_or_unknown
    SELECT tariff_or_unknown, PERCENTILE_APPROX(tariff_valid_to_to_date_max_rate_to_rows_in_tari, 0.5) AS median_tariff_valid_to_to_date_max_rate_to_rows_
    FROM enriched
    GROUP BY tariff_or_unknown
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_tariff_valid_to_to_date_max_rate_to_rows_,
        ROUND(s.tariff_valid_to_to_date_max_rate_to_rows_in_tari - g.median_tariff_valid_to_to_date_max_rate_to_rows_, 2) AS tariff_valid_to_to_date_max_rate_to_rows_in_tari_2
    FROM enriched s
    JOIN tariff_or_unknown_stats g ON g.tariff_or_unknown = s.tariff_or_unknown
    WHERE s.tariff_valid_to_to_date_max_rate_to_rows_in_tari >= g.median_tariff_valid_to_to_date_max_rate_to_rows_
)
SELECT
    tariff,
    tariff_valid_to_to_date_max_rate_to_rows_in_tari,
    first_valid_to,
    first_valid_to_label,
    tariff_slug,
    tariff_or_unknown,
    tariff_valid_to_to_date_max_rate_to_rows_in_tari_2,
    DATE_TRUNC('WEEK', first_valid_to) AS first_valid_to_week,
    LOWER(REGEXP_REPLACE(TRIM(tariff_or_unknown), ' ', '_')) AS tariff_or_unknown_slug
FROM outliers
