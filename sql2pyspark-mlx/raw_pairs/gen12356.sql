SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS row_num FROM stores WHERE region IN ('North', 'West')) AS ranked WHERE row_num <= 3 ORDER BY region
