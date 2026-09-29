select id, rating, dense_rank() over (order by rating) as rating_dense_rank
from suppliers
where supplier_name like 'Gl%'
order by id asc, rating asc;
