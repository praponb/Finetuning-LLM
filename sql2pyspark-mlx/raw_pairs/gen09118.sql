SELECT status, SUM(shipping_fee) AS total_shipping_fee, ROUND(SUM(shipping_fee) * 100.0 / SUM(SUM(shipping_fee)) OVER (), 2) AS pct_of_total FROM orders GROUP BY status
