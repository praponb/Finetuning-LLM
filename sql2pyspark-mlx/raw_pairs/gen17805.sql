SELECT id, quantity, ROW_NUMBER() OVER (ORDER BY quantity, id) AS quantity_row FROM inventory WHERE quantity BETWEEN 82 AND 169;
