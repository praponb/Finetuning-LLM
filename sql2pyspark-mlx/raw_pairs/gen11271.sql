SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating NULLS FIRST) AS latest_rank FROM suppliers) t WHERE latest_rank = 1;
