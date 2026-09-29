select id, sqft, store_name from stores where year(opened_date) = 2025 except select id, sqft, store_name from stores where region = 'West';
