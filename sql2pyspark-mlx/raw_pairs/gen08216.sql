SELECT COUNT(*) AS num_products FROM products WHERE product_name IS NOT NULL AND (category NOT IN ('Fashion', 'Sports', 'Books') OR SIZE(tags) >= 2) ORDER BY num_products;
