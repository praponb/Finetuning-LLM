select
    date_trunc('quarter', event_time) as quarter_start,
    bit_or(duration_sec) as duration_sec_bit_or,
    round(sum(duration_sec) / nullif(count(case when event_type = 'click' then 1 end), 0), 2) as duration_sec_per_click,
    round(sum(duration_sec) / nullif(count(case when page = '/product' then 1 end), 0), 2) as duration_sec_per_product
from web_events
group by 1
having duration_sec_bit_or > 1
order by duration_sec_per_click desc;
