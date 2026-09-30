select id, rating from (select *, row_number() over (partition by city order by rating desc, id) as latest_rank from suppliers) t where latest_rank = 1
