select id, price, explode(tags) as label from products where brand = 'Acme';
