SELECT segment, is_active, ROUND(STDDEV(credit_limit), 2) stddev_credit_limit FROM customers GROUP BY ROLLUP(segment, is_active);
