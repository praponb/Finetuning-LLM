select id, merchant, amount from (select *, dense_rank() over (partition by merchant order by amount desc) as rnk from transactions) as t where rnk <= 5
