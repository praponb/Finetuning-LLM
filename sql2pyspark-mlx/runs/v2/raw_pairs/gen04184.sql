select
    id,
    category,
    length(category) as category_length,
    cast(stock_qty as double) as stock_qty_num
from products
where price < 870;
