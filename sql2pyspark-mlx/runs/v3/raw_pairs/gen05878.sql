SELECT id, supplier_name FROM suppliers WHERE city IN ('Tokyo', 'New York') INTERSECT SELECT id, supplier_name FROM suppliers WHERE country IN ('UK', 'DE') ORDER BY id;
