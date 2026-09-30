SELECT country, browser, INITCAP(TRIM(browser)) AS browser_title FROM sessions WHERE browser = 'Edge' ORDER BY browser_title;
