select id, category, price, dense_rank() over (partition by category order by price) as dense_rnk from products;
