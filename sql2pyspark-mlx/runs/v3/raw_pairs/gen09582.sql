select id, device, upper(browser) as browser_upper, length(browser) as browser_length from sessions order by device, browser_upper;
