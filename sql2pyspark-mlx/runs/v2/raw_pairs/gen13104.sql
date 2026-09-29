select concat_ws(', ', sort_array(collect_set(merchant))) as merchant_values
from transactions
order by merchant_values desc;
