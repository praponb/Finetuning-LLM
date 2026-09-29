select id, status, amount, rank() over (partition by status order by amount desc) as rnk from payments;
