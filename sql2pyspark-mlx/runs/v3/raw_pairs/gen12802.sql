with ranked_store_sales as (select *, row_number() over (partition by store_id order by units desc, id) as rn from store_sales) select * from ranked_store_sales where rn <= 3 order by sale_date
