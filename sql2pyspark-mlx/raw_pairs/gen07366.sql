WITH RankedCustomers AS (SELECT id, city, age, segment, RANK() OVER (PARTITION BY city ORDER BY age) rnk FROM customers) SELECT * FROM RankedCustomers WHERE rnk <= 2
