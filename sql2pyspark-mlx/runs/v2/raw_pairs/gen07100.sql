select
    id,
    browser,
    started_at,
    country,
    case when browser = 'Chrome' then 1 else 0 end as is_chrome
from sessions
where customer_id is not null
order by browser desc;
