select id, shipped_date, status, order_id, concat(status, ' - ', carrier) as status_carrier from shipments where carrier = 'Kerry' and shipped_date between '2024-03-01' and '2024-10-01';
