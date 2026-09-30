SELECT
    id,
    started_at,
    browser,
    WEEKOFYEAR(started_at) AS started_at_weekofyear
FROM sessions;
