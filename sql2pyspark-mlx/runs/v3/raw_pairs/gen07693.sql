SELECT * FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY stock_qty DESC) rnk FROM products) ranked WHERE rnk <= 3 ORDER BY rnk DESC, price DESC
