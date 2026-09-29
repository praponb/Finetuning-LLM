WITH scoped AS (
    -- Scope the tariff_windows rows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        DATE_FORMAT(valid_to, 'yyyy-MM-dd HH:00') AS valid_to_label
    FROM tariff_windows
    WHERE valid_from < TIMESTAMP '2025-03-09 04:00:00'
      AND rate > 5.2
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_to_label,
        ROUND(AVG(rate) OVER (PARTITION BY YEAR(valid_to) ORDER BY valid_to, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rate_avg_4,
        COUNT(*) OVER (PARTITION BY tariff) AS rows_in_tariff,
        NTILE(3) OVER (PARTITION BY TO_DATE(valid_to), tariff ORDER BY rate, id) AS rate_tercile,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY tariff), 0), 4) AS rate_share_of_tariff
    FROM scoped
),
latest AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        rate,
        valid_to_label,
        rate_avg_4,
        rows_in_tariff,
        rate_tercile,
        rate_share_of_tariff
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY rows_in_tariff DESC, id DESC) AS recency_rank
        FROM with_trends
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    tariff,
    valid_from,
    rate,
    rows_in_tariff,
    rate_tercile,
    rate_share_of_tariff,
    LEAST(rows_in_tariff, rate_share_of_tariff) AS least_rows_in_tariff_rate_share_of_tariff
FROM latest
ORDER BY rows_in_tariff
