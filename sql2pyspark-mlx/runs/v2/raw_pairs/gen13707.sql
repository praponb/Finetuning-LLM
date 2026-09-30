select c.id, c.segment, c.country
from customers c
where c.country in ('SG', 'UK', 'TH') and not exists (select 1 from subscriptions s where s.customer_id = c.id);
