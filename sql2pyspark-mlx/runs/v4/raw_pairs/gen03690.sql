select *
from customers
qualify row_number() over (partition by segment order by credit_limit desc nulls last, id) = 1
