SELECT id, store_name, city, region FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS dedup_rank FROM stores) t WHERE dedup_rank = 1
