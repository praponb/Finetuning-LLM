SELECT x.segment, y.browser FROM (SELECT DISTINCT segment FROM customers) x CROSS JOIN (SELECT DISTINCT browser FROM sessions) y;
