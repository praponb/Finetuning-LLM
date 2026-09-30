select id, order_id, case when unit_price >= 390 then 'High' when unit_price >= 100 then 'Medium' else 'Low' end unit_price_band
from order_items
where quantity != 3
