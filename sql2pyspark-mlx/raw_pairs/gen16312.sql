select id, carrier, status, shipped_date, round(cost * 0.9, 2) as cost_adjusted from shipments where delivered_date > '2025-02-01' order by id desc
