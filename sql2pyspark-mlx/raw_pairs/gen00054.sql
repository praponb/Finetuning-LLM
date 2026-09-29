SELECT c.id, c.customer_id, pos, item.product_id, item.promo FROM carts c LATERAL VIEW OUTER POSEXPLODE(c.items) li AS pos, item
