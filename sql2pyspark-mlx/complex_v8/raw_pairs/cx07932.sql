WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        TO_DATE(valid_from) AS valid_from_date,
        LENGTH(TRIM(tariff)) AS tariff_len
    FROM tariff_windows
    WHERE tariff <> 'TOU'
      AND MONTH(valid_from) IN (2, 3)
),
deduped AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        valid_from_date,
        tariff_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY rate DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
tariff_stats AS (
    -- Avg rate per tariff
    SELECT tariff, AVG(rate) AS avg_rate
    FROM deduped
    GROUP BY tariff
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_rate, ROUND(s.rate - g.avg_rate, 2) AS rate_minus_avg
    FROM deduped s
    JOIN tariff_stats g ON g.tariff = s.tariff
    WHERE s.rate > g.avg_rate
)
SELECT id, measure, amount
FROM (
    SELECT id, rate_minus_avg, rate
    FROM above_average
) src
UNPIVOT (
    amount FOR measure IN (rate_minus_avg AS m_rate_minus_avg, rate AS rate_v)
) unp
