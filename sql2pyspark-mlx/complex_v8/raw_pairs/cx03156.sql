WITH prepared AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        rate
    FROM tariff_windows
    WHERE MONTH(valid_to) IN (2, 3)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM prepared
    WHERE rate < (SELECT MAX(rate) FROM prepared)
),
with_activity_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE rate > (SELECT AVG(rate) FROM with_activity)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        valid_from,
        RANK() OVER (PARTITION BY tariff ORDER BY valid_from ASC) AS valid_from_rank,
        ROUND(AVG(rate) OVER (PARTITION BY tariff), 2) AS tariff_avg_rate,
        DENSE_RANK() OVER w_tariff_by_valid_from_ties AS valid_from_dense_rank,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY tariff), 0), 4) AS rate_share_of_tariff
    FROM with_activity_2
    WINDOW w_tariff_by_valid_from_ties AS (PARTITION BY tariff ORDER BY valid_from ASC)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE valid_from_dense_rank <= 3
),
leaders_2 AS (
    -- Top 3 per group
    SELECT *
    FROM leaders
    WHERE valid_from_rank <= 3
      AND tariff_avg_rate >= 3.8
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        valid_from + INTERVAL 1 DAYS AS valid_from_plus_1d,
        DATE_FORMAT(valid_from, 'yyyy-MM') AS valid_from_label
    FROM leaders_2
)
-- Final projection
SELECT
    id,
    valid_from,
    valid_from_rank,
    tariff_avg_rate,
    valid_from_dense_rank,
    rate_share_of_tariff,
    valid_from_plus_1d,
    valid_from_label
FROM enriched
ORDER BY valid_from_rank DESC, valid_from_dense_rank NULLS LAST, id
LIMIT 10
