SELECT *, CEIL(cost) cost_whole, ADD_MONTHS(launch_date, 3) launch_date_plus_months FROM products WHERE SIZE(tags) >= 2 AND brand != 'Acme';
