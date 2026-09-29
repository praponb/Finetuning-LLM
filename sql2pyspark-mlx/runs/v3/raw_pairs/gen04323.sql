SELECT *, COALESCE(city, 'N/A') city_filled
FROM suppliers
WHERE UPPER(TRIM(supplier_name)) = 'TOKYO TRADERS' AND city = 'London'
ORDER BY id DESC, supplier_name ASC;
