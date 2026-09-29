select opened_date as event_date, 'account' as record_type
from accounts
where opened_date is not null
union all
select sale_date as event_date, 'store_sale' as record_type
from store_sales
where sale_date is not null;
