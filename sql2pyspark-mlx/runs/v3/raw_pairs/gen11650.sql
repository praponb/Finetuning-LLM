SELECT signup_date, email, credit_limit * age AS credit_limit_times_age FROM customers WHERE name NOT LIKE '%Dia%' AND country IN ('UK', 'TH') AND city IN ('London', 'Berlin');
