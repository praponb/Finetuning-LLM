SELECT region, COUNT(*) num_stores FROM stores GROUP BY region HAVING COUNT(*) > 4
