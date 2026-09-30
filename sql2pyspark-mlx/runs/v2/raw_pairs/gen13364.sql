with numbered as (select *, row_number() over (partition by status order by discount, id) as pos from orders) select id, status, discount from numbered where pos <= 3 order by 3 desc, status desc;
