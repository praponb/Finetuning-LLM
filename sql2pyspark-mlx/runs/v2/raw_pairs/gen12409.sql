SELECT sqft, region, ADD_MONTHS(opened_date, 6) AS opened_date_plus_months FROM stores WHERE (store_name LIKE '%Flagshi%' AND opened_date <= '2024-11-21') OR region IN ('North', 'South', 'West');
