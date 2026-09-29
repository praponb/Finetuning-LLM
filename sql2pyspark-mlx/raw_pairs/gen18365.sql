SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS latest_rank
FROM suppliers
QUALIFY latest_rank = 1;
