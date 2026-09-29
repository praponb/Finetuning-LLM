select launch_date from products where supplier_id is null union all select signup_date as launch_date from customers;
