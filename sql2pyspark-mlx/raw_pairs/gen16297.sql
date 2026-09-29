SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age NULLS FIRST, id) AS rank_in_group FROM customers) ranked WHERE rank_in_group = 1
