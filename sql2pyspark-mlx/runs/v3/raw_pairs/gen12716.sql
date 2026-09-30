select id, city, rating, dense_rank() over (partition by city order by rating desc) as rating_dense_rank, dense_rank() over (order by rating desc) as dense_rnk from suppliers where rating >= 3.4;
