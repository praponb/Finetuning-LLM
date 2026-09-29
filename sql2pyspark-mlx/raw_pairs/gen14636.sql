SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft ASC NULLS FIRST, id ASC) AS rank_in_group FROM stores) ranked WHERE rank_in_group = 1
