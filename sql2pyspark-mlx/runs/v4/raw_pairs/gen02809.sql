select id, country, rating, round(rating / sum(rating) over (partition by country), 3) as rating_share from suppliers where rating between 2.5 and 3.4
