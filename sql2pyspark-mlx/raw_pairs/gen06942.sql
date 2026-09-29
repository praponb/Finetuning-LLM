SELECT src.id, pos + 1 AS item_no, item.product_id, item.qty, item.promo FROM carts src LATERAL VIEW OUTER POSEXPLODE(src.items) t AS pos, item;
