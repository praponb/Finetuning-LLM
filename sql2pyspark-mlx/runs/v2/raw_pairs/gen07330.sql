SELECT *, UPPER(page) AS page_upper, TRIM(page) AS clean_page
FROM web_events;
