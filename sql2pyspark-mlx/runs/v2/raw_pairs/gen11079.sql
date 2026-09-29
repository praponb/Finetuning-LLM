SELECT id, country, INITCAP(TRIM(country)) AS country_title FROM sessions ORDER BY country_title DESC, id DESC LIMIT 3
