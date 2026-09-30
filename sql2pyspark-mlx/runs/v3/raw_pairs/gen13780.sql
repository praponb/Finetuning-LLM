select price, coalesce(price, 0) as price_filled from products where stock_qty between 131 and 399 and (array_contains(tags, 'bestseller') or array_contains(tags, 'premium'));
