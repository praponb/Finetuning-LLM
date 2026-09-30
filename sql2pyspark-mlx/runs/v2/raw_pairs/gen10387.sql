select id, rating from suppliers where rating >= (select avg(rating) from suppliers where city <> 'Chiang Mai') order by id desc;
