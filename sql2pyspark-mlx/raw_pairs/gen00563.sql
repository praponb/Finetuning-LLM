select id, delivered_date, cost from shipments where carrier = 'DHL' and (month(delivered_date) in (1, 3, 11) or cost between 64 and 91);
