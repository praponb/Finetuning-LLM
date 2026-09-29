select id, stock_qty from products where supplier_id in (13, 36) intersect select id, stock_qty from products where launch_date >= '2023-05-01'
