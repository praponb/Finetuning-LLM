select
    ss.store_id,
    p.product_name,
    p.launch_date,
    s.opened_date,
    date_sub(ss.sale_date, 27) as sale_date_minus_27d
from store_sales ss
right outer join products p on ss.product_id = p.id
left outer join stores s on ss.store_id = s.id
where s.opened_date between '2024-04-01' and '2025-02-01'
order by sale_date_minus_27d, opened_date;
