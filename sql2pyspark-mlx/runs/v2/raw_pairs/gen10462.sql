SELECT *, ARRAY_CONTAINS(tags, 'new') AS is_new FROM products WHERE launch_date <= '2023-04-01'
