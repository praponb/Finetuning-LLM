SELECT id, price, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE cost BETWEEN 580 AND 670;
