WITH recent_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        valid_from,
        valid_to,
        rate
    FROM tariff_windows
    WHERE rate >= 4.2
      AND MONTH(valid_to) IN (1, 3)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        valid_from,
        valid_to,
        rate,
        NTH_VALUE(valid_to, 2) OVER (ORDER BY valid_to DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_valid_to,
        LAST_VALUE(valid_from) OVER (ORDER BY valid_to DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_valid_from,
        ROUND(rate / NULLIF(SUM(rate) OVER (PARTITION BY TO_DATE(valid_from)), 0), 4) AS rate_share_of_valid_from_to_date,
        FIRST_VALUE(rate) OVER (ORDER BY valid_to DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_rate
    FROM recent_tariffs
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, rate_share_of_valid_from_to_date, first_rate
    FROM ranked
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (rate_share_of_valid_from_to_date, first_rate)
) unp
