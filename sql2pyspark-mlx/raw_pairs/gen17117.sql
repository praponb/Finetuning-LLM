with ranked_store_sales as (select *, row_number() over (partition by product_id order by sale_date desc, id) as rnk from store_sales) select * from ranked_store_sales where rnk <= 3;
