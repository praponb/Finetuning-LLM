SELECT country FROM sessions WHERE browser IN ('Edge', 'Chrome', 'Safari') UNION ALL SELECT country FROM sessions WHERE country IS NOT NULL;
