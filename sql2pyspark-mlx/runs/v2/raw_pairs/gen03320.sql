select id, city, rating, round(rating / sum(rating) over (partition by city), 3) as rating_share from suppliers order by rating desc;
