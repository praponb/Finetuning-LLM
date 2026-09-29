select *, case when country = 'DE' then 1 else 0 end as is_de, lower(browser) as browser_lower from sessions where country != 'DE'
