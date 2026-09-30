select s.*
from orders s
where exists (
        select 1
        from shipments c
        where c.order_id = s.id
          and c.shipped_date <= s.order_date
    );
