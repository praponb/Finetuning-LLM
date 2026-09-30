select
    id,
    category,
    supplier_id,
    stock_qty,
    rank() over (partition by category, supplier_id order by stock_qty desc) stock_qty_rank,
    count(*) over (partition by category) category_count
from products
where category <> 'Home'
order by category, supplier_id desc;
