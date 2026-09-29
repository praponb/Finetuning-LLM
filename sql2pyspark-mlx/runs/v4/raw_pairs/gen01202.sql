WITH valid AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        TRIM(country) AS clean_country,
        DAY(started_at) AS started_at_day
    FROM sessions
    WHERE country IS NOT NULL
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at_day DESC NULLS FIRST, id) AS dedup_rank
    FROM valid
) ranked
WHERE dedup_rank = 1
