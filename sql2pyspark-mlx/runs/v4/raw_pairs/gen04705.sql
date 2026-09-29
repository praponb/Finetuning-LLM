with numbered as (select *, row_number() over (partition by city order by sqft desc, id) as rnk from stores where store_name is not null) select * from numbered where rnk <= 5;
