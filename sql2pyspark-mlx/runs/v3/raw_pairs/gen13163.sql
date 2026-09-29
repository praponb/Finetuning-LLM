select id, carrier, cost, max(cost) over (partition by carrier) as carrier_max_cost from shipments where delivered_date > '2025-10-18';
