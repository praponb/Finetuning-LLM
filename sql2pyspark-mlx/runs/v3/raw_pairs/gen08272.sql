SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rnk FROM suppliers) AS r WHERE rnk <= 2 ORDER BY rating DESC, country
