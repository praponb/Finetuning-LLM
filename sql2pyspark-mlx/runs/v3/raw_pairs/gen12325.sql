select id, supplier_id, stock_qty, brand from (select id, supplier_id, stock_qty, brand, row_number() over (partition by supplier_id order by stock_qty desc, id) pos from products) t where pos <= 3;
