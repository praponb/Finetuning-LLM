with ranked as (select id, brand, cost, price, row_number() over (partition by brand order by cost desc, id) as rnk from products) select id, brand, cost, price from ranked where rnk = 1;
