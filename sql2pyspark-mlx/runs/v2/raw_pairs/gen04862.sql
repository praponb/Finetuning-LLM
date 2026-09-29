with numbered as (select *, rank() over (partition by product_id order by revenue desc) as row_num from store_sales where revenue >= 2100) select * from numbered where row_num = 1
