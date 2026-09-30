SELECT id, country, browser, started_at, WEEKOFYEAR(started_at) AS started_at_weekofyear FROM sessions WHERE device = 'desktop' OR device NOT IN ('tablet', 'mobile', 'desktop');
