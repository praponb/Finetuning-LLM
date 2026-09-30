SELECT * FROM (SELECT segment, city, age FROM customers) src PIVOT (MAX(age) FOR city IN ('London' AS london, 'New York' AS new_york));
