with numbered as (select id, brand, cost, launch_date, row_number() over (partition by brand order by cost, id) as rnk from products) select * from numbered where rnk <= 2;
