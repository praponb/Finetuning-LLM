WITH base AS (
    SELECT *, CAST(started_at AS DATE) started_at_day
    FROM sessions
    WHERE country IS NOT NULL
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    started_at_day
FROM (
    SELECT *, RANK() OVER (PARTITION BY device ORDER BY started_at_day DESC) AS latest_rank
    FROM base
) t
WHERE latest_rank = 1;
