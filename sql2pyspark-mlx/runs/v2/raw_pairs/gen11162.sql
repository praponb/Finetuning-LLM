select round(stddev(duration_sec), 2) as stddev_duration_sec from web_events where event_type <> 'click' order by stddev_duration_sec desc;
