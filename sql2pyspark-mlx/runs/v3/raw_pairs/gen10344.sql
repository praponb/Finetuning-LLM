SELECT *, INITCAP(TRIM(page)) page_title, SUBSTRING(page, 1, 4) page_prefix FROM web_events WHERE HOUR(event_time) BETWEEN 6 AND 9 OR duration_sec > 307;
