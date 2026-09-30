WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(name) AS name_lower,
        LENGTH(name) AS name_length,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM customers
    WHERE age = 38
      AND city IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at >= s.signup_date
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date < s.signup_date
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        segment,
        credit_limit,
        age,
        LAG(age, 2) OVER (PARTITION BY segment, city ORDER BY credit_limit, id) AS prev_age,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM kept
    WHERE segment <> 'Consumer'
    WINDOW w_recent AS (PARTITION BY city ORDER BY credit_limit, id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        segment,
        credit_limit,
        age,
        event_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY prev_age DESC NULLS LAST, id) AS rank_in_group
        FROM enriched
    ) x
    WHERE rank_in_group = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        credit_limit + event_seq AS credit_limit_plus_event_seq,
        COALESCE(age, 0) AS age_filled
    FROM ranked
    WHERE event_seq <= 7
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.event_seq,
        s.credit_limit_plus_event_seq,
        latest.subscription_monthly_fee
    FROM valid s
    JOIN LATERAL (
            SELECT c.monthly_fee AS subscription_monthly_fee
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    APPROX_PERCENTILE(credit_limit_plus_event_seq, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_plus_event_seq_quartiles,
    PERCENTILE_APPROX(credit_limit_plus_event_seq, 0.5) AS credit_limit_plus_event_seq_p50
FROM with_last
GROUP BY ROLLUP (CASE WHEN event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END)
HAVING PERCENTILE_APPROX(credit_limit_plus_event_seq, 0.5) >= 0
ORDER BY event_seq_tier DESC NULLS FIRST;
