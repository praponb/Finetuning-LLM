SELECT id, status, delivered_date, carrier FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date, id) AS rnk FROM shipments WHERE carrier = 'Kerry') AS ranked WHERE rnk <= 3;
