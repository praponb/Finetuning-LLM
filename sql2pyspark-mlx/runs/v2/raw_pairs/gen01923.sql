with numbered as (select *, row_number() over (partition by region order by opened_date desc, id) as rn from stores) select id, region, opened_date, store_name from numbered where rn = 1;
