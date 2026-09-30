select *, trim(browser) as clean_browser, case when device = 'tablet' then 1 else 0 end as is_tablet
from sessions
where country <> 'UK' or device = 'desktop';
