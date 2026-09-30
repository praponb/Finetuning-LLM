SELECT started_at, YEAR(started_at) AS started_at_year, LENGTH(country) AS country_length FROM sessions WHERE device = 'desktop' AND (browser = 'Chrome' OR country <> 'SG') ORDER BY started_at_year
