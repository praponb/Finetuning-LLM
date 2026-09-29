select id, brand, cost, round(cost / sum(cost) over (partition by brand), 3) as cost_share from products
