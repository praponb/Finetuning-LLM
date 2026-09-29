SELECT
    opened_date,
    region,
    store_name,
    SUBSTRING(store_name, 1, 2) AS store_name_prefix,
    DATE_SUB(opened_date, 25) AS opened_date_minus_25d
FROM stores
WHERE NOT (opened_date > '2023-12-01' AND city IN ('New York', 'Bangkok') AND sqft IS NULL);
