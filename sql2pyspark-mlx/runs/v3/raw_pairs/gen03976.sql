select * from (select *, dense_rank() over (partition by country order by rating) as row_num from suppliers where country = 'TH') as ranked where row_num = 1
