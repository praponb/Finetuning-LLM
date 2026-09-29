select id, customer_id, started_at, device, upper(browser) as browser_upper, case when country = 'US' then 1 else 0 end as is_us from sessions where browser = 'Safari' and customer_id is not null;
