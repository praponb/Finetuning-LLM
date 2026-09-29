SELECT a.country, b.device FROM (SELECT DISTINCT country FROM suppliers) a CROSS JOIN (SELECT DISTINCT device FROM sessions) b
