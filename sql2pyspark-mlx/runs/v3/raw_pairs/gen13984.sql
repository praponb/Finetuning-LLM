SELECT id, brand, category, EXPLODE(tags) AS label FROM products WHERE stock_qty >= 360;
