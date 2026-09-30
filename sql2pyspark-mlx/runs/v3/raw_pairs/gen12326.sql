SELECT *, credit_limit + age AS credit_limit_plus_age, CASE WHEN country IN ('TH', 'DE') THEN 'Core' ELSE 'Other' END AS country_group FROM customers;
