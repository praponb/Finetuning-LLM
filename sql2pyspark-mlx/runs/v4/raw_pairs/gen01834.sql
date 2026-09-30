select id, customer_id, elem.product_id, elem.price, elem.promo from (select id, customer_id, explode_outer(items) as elem from carts) x
