WITH store_summary AS (
    SELECT store_id, SORT_ARRAY(COLLECT_SET(status)) status_list, SUM(CASE WHEN channel = 'partner' THEN amount ELSE 0 END) partner_amount
    FROM orders
    GROUP BY store_id
)
SELECT s.sqft, ss.status_list, ss.partner_amount
FROM stores s
INNER JOIN store_summary ss ON s.id = ss.store_id
WHERE ss.partner_amount > 37
ORDER BY sqft;
