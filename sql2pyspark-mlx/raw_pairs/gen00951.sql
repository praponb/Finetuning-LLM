SELECT *, REGEXP_REPLACE(store_name, ' ', '-') AS store_name_slug
FROM stores
WHERE MONTH(opened_date) IN (5, 10, 12) AND sqft BETWEEN 7057 AND 13799;
