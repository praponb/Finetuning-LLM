SELECT sqft, city, region, CONCAT(region, ' - ', city) AS region_city FROM stores WHERE city NOT IN ('Singapore', 'London', 'Bangkok');
