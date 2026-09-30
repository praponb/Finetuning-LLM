SELECT page, SUM(duration_sec) AS total_duration_sec, SUM(CASE WHEN event_type = 'click' THEN duration_sec ELSE 0 END) AS click_duration_sec FROM web_events GROUP BY page
