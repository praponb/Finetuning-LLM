select id, quantity, row_number() over (order by quantity, id) quantity_row from order_items
