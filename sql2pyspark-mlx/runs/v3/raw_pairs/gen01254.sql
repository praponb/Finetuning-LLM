SELECT brand, cost FROM products WHERE ARRAY_CONTAINS(tags, 'eco') AND cost <= 440;
