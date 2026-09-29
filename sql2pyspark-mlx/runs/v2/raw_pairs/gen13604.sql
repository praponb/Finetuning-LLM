SELECT browser, INITCAP(TRIM(device)) AS device_title, INITCAP(TRIM(device)) AS device_title_2 FROM sessions WHERE device = 'mobile' ORDER BY device_title_2 DESC;
