select id, price, product_name, explode(tags) as label from products where brand not in ('Initech', 'Acme', 'Umbrella');
