SELECT browser, SIZE(COLLECT_SET(country)) AS num_countries, SORT_ARRAY(COLLECT_SET(device)) AS device_list, GROUPING(browser) AS grouping_id FROM sessions GROUP BY CUBE (browser);
