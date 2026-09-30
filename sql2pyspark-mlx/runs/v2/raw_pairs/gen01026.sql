SELECT opened_date, city FROM stores WHERE region IN ('Central', 'South', 'East') UNION SELECT opened_date, city FROM stores WHERE YEAR(opened_date) = 2025;
