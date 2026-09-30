SELECT id, segment, age, AVG(age) OVER (PARTITION BY segment) AS segment_avg_age FROM customers;
