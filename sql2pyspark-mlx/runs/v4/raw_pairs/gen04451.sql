SELECT opened_date FROM stores WHERE city <> 'Berlin' UNION SELECT opened_date FROM stores WHERE opened_date BETWEEN '2025-05-08' AND '2025-08-01';
