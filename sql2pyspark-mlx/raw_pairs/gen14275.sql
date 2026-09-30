select *, rank() over (partition by brand order by stock_qty desc nulls first) as row_num
from products
qualify row_num = 1;
