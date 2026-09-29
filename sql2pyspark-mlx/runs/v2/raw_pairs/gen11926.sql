SELECT stock_qty, cost, price, COALESCE(price, 0) AS price_filled, SORT_ARRAY(tags) AS sorted_tags FROM products ORDER BY 4 DESC, price;
