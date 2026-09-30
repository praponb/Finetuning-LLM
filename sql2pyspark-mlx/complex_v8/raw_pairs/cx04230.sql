WITH scoped_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        ROUND((UNIX_TIMESTAMP(valid_to) - UNIX_TIMESTAMP(valid_from)) / 3600, 1) AS hours_valid_from_to_valid_to,
        DATE_TRUNC('YEAR', valid_to) AS valid_to_year
    FROM tariff_windows
    WHERE valid_to >= TIMESTAMP '2025-02-18 04:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER w_valid_from_month_valid_to_ye AS rows_in_valid_from_month_valid_to_year_to_date,
        MIN(hours_valid_from_to_valid_to) OVER (PARTITION BY TO_DATE(valid_to), tariff ORDER BY hours_valid_from_to_valid_to NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_hours_valid_from_to_valid_to_so_far,
        ROW_NUMBER() OVER (ORDER BY valid_to ASC, id) AS rn,
        ROUND(hours_valid_from_to_valid_to / NULLIF(SUM(hours_valid_from_to_valid_to) OVER (PARTITION BY MONTH(valid_from), TO_DATE(valid_to_year)), 0), 4) AS hours_valid_from_to_valid_to_share_of_valid_from,
        LEAD(valid_to) OVER (ORDER BY valid_from, id) AS next_valid_to,
        NTILE(10) OVER (PARTITION BY TO_DATE(valid_to), tariff ORDER BY hours_valid_from_to_valid_to NULLS LAST, id) AS hours_valid_from_to_valid_to_decile,
        ROUND(hours_valid_from_to_valid_to / NULLIF(SUM(hours_valid_from_to_valid_to) OVER (PARTITION BY TO_DATE(valid_to)), 0), 4) AS hours_valid_from_to_valid_to_share_of_valid_to_t,
        LEAD(valid_from) OVER (PARTITION BY tariff ORDER BY hours_valid_from_to_valid_to, id) AS next_valid_from,
        RANK() OVER (PARTITION BY TO_DATE(valid_to), tariff ORDER BY hours_valid_from_to_valid_to NULLS LAST) AS hours_valid_from_to_valid_to_rank,
        FIRST_VALUE(valid_to_year) IGNORE NULLS OVER (PARTITION BY TO_DATE(valid_to), tariff ORDER BY hours_valid_from_to_valid_to NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_valid_to_year,
        LAG(valid_to_year) OVER (ORDER BY valid_from, id) AS prev_valid_to_year,
        MAX(hours_valid_from_to_valid_to) OVER (PARTITION BY MONTH(valid_from), TO_DATE(valid_to_year)) AS valid_from_month_valid_to_year_to_date_max_hours,
        DENSE_RANK() OVER (PARTITION BY tariff ORDER BY hours_valid_from_to_valid_to) AS hours_valid_from_to_valid_to_dense_rank,
        ROUND(AVG(hours_valid_from_to_valid_to) OVER w_valid_to_to_date, 2) AS valid_to_to_date_avg_hours_valid_from_to_valid_t
    FROM scoped_tariffs
    WINDOW w_valid_from_month_valid_to_ye AS (PARTITION BY MONTH(valid_from), TO_DATE(valid_to_year)), w_valid_to_to_date AS (PARTITION BY TO_DATE(valid_to))
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        MAX(valid_from) AS last_valid_from,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN tariff IS NOT NULL THEN 1 ELSE 0 END) AS n_tariff_is_not_null,
        SUM(CASE WHEN tariff <> 'FLAT' THEN hours_valid_from_to_valid_to_share_of_valid_to_t ELSE 0 END) AS hours_valid_from_to_valid_to_share_of_valid_to_t_2,
        SUM(CASE WHEN tariff = 'FLAT' THEN 1 ELSE 0 END) AS n_tariff_flat,
        COUNT(*) AS cnt
    FROM windowed
    GROUP BY 1
)
SELECT
    valid_from_month,
    last_valid_from,
    distinct_ids,
    n_tariff_is_not_null,
    hours_valid_from_to_valid_to_share_of_valid_to_t_2,
    cnt
FROM grouped
ORDER BY distinct_ids NULLS LAST
