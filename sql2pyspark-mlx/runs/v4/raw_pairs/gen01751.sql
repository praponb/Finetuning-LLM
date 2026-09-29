SELECT id, customer_id, coupon_codes, pos, item.product_id, item.qty, item.price FROM carts LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, item
