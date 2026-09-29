SELECT sqft, region, DATE_ADD(opened_date, 15) opened_date_plus_15d
FROM stores
ORDER BY opened_date_plus_15d, sqft DESC
