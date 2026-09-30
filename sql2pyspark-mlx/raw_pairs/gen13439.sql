select browser, 'session' as record_type
from sessions
union
select city as browser, 'supplier' as record_type
from suppliers
order by record_type;
