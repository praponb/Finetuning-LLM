select
    id,
    device,
    substring(browser, 1, 3) as browser_prefix,
    case when country = 'SG' then 1 else 0 end as is_sg
from sessions
where country <> 'TH' and country in ('JP', 'TH') and device in ('tablet', 'mobile', 'desktop');
