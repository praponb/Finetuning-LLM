SELECT city FROM suppliers WHERE country = 'US' INTERSECT SELECT city FROM stores WHERE opened_date BETWEEN '2023-12-01' AND '2025-09-01'
