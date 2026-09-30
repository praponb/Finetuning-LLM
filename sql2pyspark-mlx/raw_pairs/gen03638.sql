SELECT
    id,
    supplier_id,
    cost,
    launch_date,
    stock_qty,
    MIN(cost) OVER (PARTITION BY supplier_id) supplier_id_min_cost,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), supplier_id ORDER BY stock_qty DESC) stock_qty_dense_rank
FROM products;
