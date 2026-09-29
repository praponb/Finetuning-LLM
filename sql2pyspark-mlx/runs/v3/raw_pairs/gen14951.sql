SELECT *, UPPER(store_name) store_name_upper, COALESCE(sqft, 0) sqft_filled
FROM stores
WHERE region = 'North' AND (region NOT IN ('West', 'North') OR MONTH(opened_date) IN (1, 2, 5));
