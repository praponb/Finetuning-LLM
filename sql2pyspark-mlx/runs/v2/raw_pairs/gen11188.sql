select id, category, price, round(price / sum(price) over (partition by category), 3) as price_share from products where brand in ('Stark', 'Initech', 'Globex') order by id desc
