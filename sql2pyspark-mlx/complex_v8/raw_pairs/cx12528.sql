WITH prepared_tariffs AS (
    -- Filter tariff_windows
    SELECT
        id,
        tariff,
        valid_from,
        rate,
        DATE_FORMAT(valid_to, 'yyyy-MM-dd') AS valid_to_label
    FROM tariff_windows
    WHERE tariff = 'TOU'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', valid_from) AS valid_from_month,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(rate), 2) AS total_rate,
        SUM(CASE WHEN tariff = 'EV' THEN 1 ELSE 0 END) AS n_tariff_ev,
        ARRAY_SORT(COLLECT_SET(tariff)) AS tariff_list
    FROM prepared_tariffs
    GROUP BY 1
    HAVING SUM(rate) > 2.2
),
exploded AS (
    -- One row per tariff_list_item
    SELECT
        s.valid_from_month,
        s.distinct_ids,
        s.total_rate,
        s.n_tariff_ev,
        t.tariff_list_item
    FROM stats s
    LATERAL VIEW EXPLODE(s.tariff_list) t AS tariff_list_item
),
scored AS (
    -- Derive helper columns
    SELECT
        valid_from_month,
        distinct_ids,
        total_rate,
        n_tariff_ev,
        tariff_list_item,
        TO_DATE(valid_from_month) AS valid_from_month_date,
        ABS(n_tariff_ev - 4) AS n_tariff_ev_distance
    FROM exploded
)
-- Final projection
SELECT
    valid_from_month,
    n_tariff_ev,
    tariff_list_item,
    valid_from_month_date,
    n_tariff_ev_distance,
    LEAST(n_tariff_ev_distance, total_rate) AS least_n_tariff_ev_distance_total_rate
FROM scored
WHERE n_tariff_ev >= 4
ORDER BY valid_from_month_date, valid_from_month, tariff_list_item
LIMIT 5
