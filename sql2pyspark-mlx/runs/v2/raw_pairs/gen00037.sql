select id, discount, case when amount >= 1750 then 'High' when amount >= 420 then 'Medium' else 'Low' end as amount_band
from orders
where amount between 920 and 930;
