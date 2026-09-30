SELECT
    s.delivered_date,
    o.store_id,
    o.channel,
    st.region,
    st.sqft,
    LAST_DAY(o.order_date) AS order_date_month_end
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
LEFT OUTER JOIN stores st ON o.store_id = st.id
WHERE st.region IN ('North', 'West', 'South')
ORDER BY o.store_id
