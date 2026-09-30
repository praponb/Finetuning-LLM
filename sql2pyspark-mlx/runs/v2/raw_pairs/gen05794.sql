select
    id,
    browser,
    country,
    device,
    trim(device) as clean_device
from sessions
order by browser, device
