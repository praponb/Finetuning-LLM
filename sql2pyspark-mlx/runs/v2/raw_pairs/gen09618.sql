WITH top_shipments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) AS rn FROM shipments WHERE carrier = 'Kerry') SELECT * FROM top_shipments WHERE rn <= 5;
