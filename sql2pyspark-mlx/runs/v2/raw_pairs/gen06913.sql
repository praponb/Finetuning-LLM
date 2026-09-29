select
    customer_id,
    browser,
    date_format(started_at, 'yyyy-MM') as started_at_label,
    upper(device) as device_upper
from sessions
where device = 'desktop' and device <> 'mobile';
