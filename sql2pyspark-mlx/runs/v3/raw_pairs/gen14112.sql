select id, brand, stock_qty, percent_rank() over (partition by brand order by stock_qty desc) as stock_qty_pct_rank from products
