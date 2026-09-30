select * from (select id, brand, price, row_number() over (partition by brand order by price, id) as rnk from products) as t where rnk <= 1 order by price
