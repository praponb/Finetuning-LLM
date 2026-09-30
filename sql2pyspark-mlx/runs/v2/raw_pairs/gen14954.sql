SELECT
    s.id,
    s.cost,
    s.order_id,
    o.store_id,
    st.region,
    ADD_MONTHS(s.delivered_date, 6) AS delivered_date_plus_months
FROM shipments s
RIGHT OUTER JOIN orders o ON o.id = s.order_id
FULL OUTER JOIN stores st ON st.id = o.store_id
ORDER BY 4 DESC, s.cost DESC
