SELECT ROUND(STDDEV(age), 2) AS stddev_age FROM customers WHERE segment IN ('Consumer', 'Corporate');
