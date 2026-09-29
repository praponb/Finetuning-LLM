SELECT id, launch_date FROM products WHERE launch_date > (SELECT MAX(launch_date) FROM products WHERE brand IN ('Initech', 'Stark', 'Umbrella'));
