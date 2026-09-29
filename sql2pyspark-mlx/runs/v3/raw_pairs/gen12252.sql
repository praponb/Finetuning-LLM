SELECT id, country, CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk FROM sessions WHERE browser IN ('Edge', 'Chrome', 'Safari') AND country IN ('JP', 'US');
