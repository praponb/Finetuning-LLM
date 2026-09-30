select
    product_id,
    case when quantity >= 194 then 'high' when quantity >= 63 then 'mid' else 'low' end as quantity_bucket,
    min(last_restock) as first_last_restock,
    max(last_restock) as last_last_restock
from inventory
group by 1, 2;
