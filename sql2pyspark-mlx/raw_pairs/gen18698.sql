with ranked as (select *, row_number() over (partition by product_id order by revenue desc, id) as rn from store_sales) select * from ranked where rn <= 2 order by sale_date;
