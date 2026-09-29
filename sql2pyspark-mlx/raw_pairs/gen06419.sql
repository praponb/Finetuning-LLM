SELECT id, device, CONCAT_WS('/', browser, country) browser_country_key, INITCAP(TRIM(device)) device_title FROM sessions WHERE browser IN ('Firefox', 'Chrome') AND device = 'mobile'
