SELECT city FROM suppliers WHERE city IN ('Singapore', 'Tokyo') UNION SELECT city FROM stores;
