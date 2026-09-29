select id, brand from products where brand is not null intersect select id, brand from products where length(product_name) >= 11
