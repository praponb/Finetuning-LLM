select id, method, amount, status from (select id, method, amount, status, rank() over (partition by method order by amount desc) as rnk from payments) as r where rnk <= 2 order by 3 desc, method asc
