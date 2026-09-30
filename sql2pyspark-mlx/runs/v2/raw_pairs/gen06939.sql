SELECT
    id,
    browser,
    DAYOFWEEK(started_at) started_at_dayofweek,
    DATE_FORMAT(started_at, 'yyyy-MM-dd') started_at_label
FROM sessions
