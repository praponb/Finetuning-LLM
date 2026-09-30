select max(quantity) as max_quantity, count(distinct quantity) as unique_stores from inventory order by max_quantity
