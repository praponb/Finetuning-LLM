SELECT id, age, ROW_NUMBER() OVER (ORDER BY age DESC, id) AS age_row
FROM customers
