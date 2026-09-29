select id, country, rating, lead(rating, 1) over (partition by country order by rating, id) as next_rating from suppliers where upper(supplier_name) like '%OKY%'
