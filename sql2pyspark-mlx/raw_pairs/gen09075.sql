SELECT opened_date FROM stores WHERE region <> 'South' UNION ALL SELECT delivered_date FROM shipments WHERE carrier IN ('FedEx', 'Flash', 'UPS')
