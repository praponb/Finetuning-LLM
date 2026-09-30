SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC NULLS LAST, id) AS latest_rank
    FROM sessions
) t
WHERE latest_rank = 1;
