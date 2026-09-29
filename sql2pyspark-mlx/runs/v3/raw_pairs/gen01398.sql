SELECT discount_bucket, avg_shipping_fee, shipped_discount
FROM (
    SELECT CASE WHEN discount >= 0.44 THEN 'high' WHEN discount >= 0.17 THEN 'mid' ELSE 'low' END discount_bucket, AVG(shipping_fee) avg_shipping_fee, SUM(CASE WHEN status = 'SHIPPED' THEN discount ELSE 0 END) shipped_discount
    FROM orders
    WHERE store_id IS NULL
    GROUP BY CASE WHEN discount >= 0.44 THEN 'high' WHEN discount >= 0.17 THEN 'mid' ELSE 'low' END
) t
WHERE shipped_discount < 41
ORDER BY shipped_discount ASC;
