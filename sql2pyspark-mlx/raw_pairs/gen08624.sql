select * from (select *, row_number() over (partition by status order by discount, id) rnk from orders) r where rnk <= 2
