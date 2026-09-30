select id, product_name, label from products lateral view explode(tags) t as label where stock_qty is null;
