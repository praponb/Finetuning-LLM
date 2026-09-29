WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DAYOFWEEK(start_date) AS start_date_dayofweek,
        DATE_TRUNC('YEAR', end_date) AS end_date_period
    FROM subscriptions
    WHERE end_date >= DATE '2023-04-06'
      AND customer_id IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY status ORDER BY end_date_period DESC NULLS FIRST) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, b.country
    FROM ranked a
    INNER JOIN customers b
        ON a.customer_id = b.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(end_date_period) OVER (PARTITION BY status ORDER BY end_date_period, id) IS NULL
                 OR end_date_period > LAG(end_date_period) OVER (PARTITION BY status ORDER BY end_date_period, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM joined
),
sessions_subscriptions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY end_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
)
-- Join a dimension and aggregate
SELECT
    dim.country AS customer_country,
    src.status,
    COUNT(*) AS rows_total,
    MIN(src.monthly_fee) AS min_monthly_fee,
    ROUND(AVG(src.is_new_session), 2) AS avg_is_new_session,
    GROUPING_ID() AS gid
FROM sessions_subscriptions src
INNER JOIN customers dim ON src.customer_id = dim.id
GROUP BY CUBE (dim.country, src.status)
