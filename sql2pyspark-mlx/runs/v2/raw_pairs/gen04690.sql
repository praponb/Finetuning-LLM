select id, brand, supplier_id from products where (category in ('Fashion', 'Home', 'Electronics') and cost between 400 and 600) or launch_date > '2023-04-07';
