SELECT * FROM (SELECT brand, category, price FROM products WHERE ARRAY_CONTAINS(tags, 'new')) PIVOT (SUM(price) FOR category IN ('Electronics', 'Fashion'));
