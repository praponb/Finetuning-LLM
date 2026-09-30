SELECT * FROM (SELECT MONTH(delivered_date) AS delivered_month, status, cost FROM shipments WHERE carrier IN ('UPS', 'Kerry'))
PIVOT (COUNT(cost) FOR status IN ('IN_TRANSIT', 'DELIVERED', 'RETURNED', 'LOST'))
