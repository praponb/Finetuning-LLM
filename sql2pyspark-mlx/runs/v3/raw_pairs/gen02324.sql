SELECT id, sqft, opened_date, DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label FROM stores WHERE opened_date BETWEEN '2024-03-01' AND '2024-05-13' OR region IN ('Central', 'East');
