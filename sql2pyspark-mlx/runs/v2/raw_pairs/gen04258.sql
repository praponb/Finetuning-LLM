WITH store_totals AS (
    SELECT store_id, COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count, SUM(CASE WHEN channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_count
    FROM orders
    WHERE customer_id = 15
    GROUP BY store_id
)
SELECT
    s.region,
    s.opened_date,
    st.web_count,
    st.mobile_count
FROM stores s
LEFT JOIN store_totals st ON s.id = st.store_id
ORDER BY mobile_count DESC, region
