SELECT
    id,
    segment,
    age,
    ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS age_row
FROM customers;
