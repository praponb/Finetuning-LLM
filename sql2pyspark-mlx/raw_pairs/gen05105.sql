SELECT sqft FROM stores WHERE sqft <= 4494 UNION ALL SELECT sqft FROM stores WHERE region NOT IN ('Central', 'North', 'South') ORDER BY sqft
