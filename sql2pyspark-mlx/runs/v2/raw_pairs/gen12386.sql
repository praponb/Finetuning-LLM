select id, browser, country, device, length(country) as country_length from sessions where country = 'JP' order by browser;
