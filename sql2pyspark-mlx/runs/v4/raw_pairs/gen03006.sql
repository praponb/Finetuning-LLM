SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at, id) AS event_seq,
    ROW_NUMBER() OVER (ORDER BY started_at, id) AS row_seq
FROM sessions;
