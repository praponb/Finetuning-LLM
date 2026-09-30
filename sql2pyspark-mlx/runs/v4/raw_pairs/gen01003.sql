SELECT * FROM (SELECT order_id, status, carrier, cost FROM shipments) src PIVOT (MAX(cost) AS value_sum, COUNT(1) AS n FOR carrier IN ('Kerry' AS kerry, 'DHL' AS dhl));
