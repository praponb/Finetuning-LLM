SELECT opened_date
FROM stores
WHERE region = 'West' AND (region = 'West' OR sqft <= 14259);
