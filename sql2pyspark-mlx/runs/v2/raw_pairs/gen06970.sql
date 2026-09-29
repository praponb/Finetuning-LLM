select *, concat(country, '/', device) country_device, date_format(started_at, 'yyyy-MM') started_at_label
from sessions
order by id desc
limit 10;
