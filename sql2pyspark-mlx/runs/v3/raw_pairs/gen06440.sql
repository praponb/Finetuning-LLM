SELECT id, country, customer_id, INITCAP(TRIM(device)) AS device_title, INITCAP(TRIM(device)) AS device_title_2 FROM sessions WHERE country NOT IN ('SG', 'JP') ORDER BY device_title DESC
