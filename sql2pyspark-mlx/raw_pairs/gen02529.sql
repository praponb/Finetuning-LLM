SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating NULLS LAST, id) AS rank_in_group FROM suppliers QUALIFY rank_in_group = 1;
