select id, supplier_id, metric, metric_value from (select id, supplier_id, price, cost from products) src unpivot (metric_value for metric in (price as PRICE_VAL, cost))
