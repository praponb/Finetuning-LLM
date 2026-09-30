SELECT a.city, b.browser FROM (SELECT DISTINCT city FROM suppliers) a CROSS JOIN (SELECT DISTINCT browser FROM sessions) b;
