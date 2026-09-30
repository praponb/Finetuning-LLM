SELECT *
FROM (
    SELECT QUARTER(shipped_date) AS shipped_quarter, SORT_ARRAY(COLLECT_SET(status)) AS status_list, MIN(cost) AS min_cost
    FROM shipments
    GROUP BY QUARTER(shipped_date)
) AS t
WHERE min_cost < 59;
