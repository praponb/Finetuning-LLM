SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity, id) AS latest_rank FROM inventory QUALIFY latest_rank = 1;
