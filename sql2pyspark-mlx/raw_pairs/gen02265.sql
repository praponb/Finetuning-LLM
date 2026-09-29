WITH with_windows AS (
    SELECT
        *,
        LAG(cost, 1) OVER w_running AS prev_cost,
        LAST_VALUE(carrier, TRUE) OVER (PARTITION BY status ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        AVG(cost) OVER (PARTITION BY YEAR(shipped_date), order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        FIRST_VALUE(carrier, TRUE) OVER w_part AS first_known_carrier
    FROM shipments
    WINDOW w_running AS (PARTITION BY carrier ORDER BY cost ASC, id ASC), w_part AS (PARTITION BY order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    status,
    cost_avg_6,
    first_known_carrier,
    CASE WHEN carrier RLIKE '^(FedEx|UPS)$' THEN 'OTHER_ZONE' WHEN carrier RLIKE '^(DHL|Kerry)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS carrier_zone
FROM with_windows
ORDER BY carrier_zone DESC, carrier DESC
