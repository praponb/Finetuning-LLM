SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date ASC, id ASC) AS rank_in_group FROM stores QUALIFY rank_in_group = 1
