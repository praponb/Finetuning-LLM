select round(avg(cost), 2) avg_cost from products where cost < 250 and brand not in ('Globex', 'Umbrella', 'Initech');
