WITH base AS (
    SELECT
        *,
        REGEXP_REPLACE(email, ' ', '-') AS email_slug,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM customers
    WHERE is_active
      AND city IS DISTINCT FROM 'Singapore'
),
enriched AS (
    SELECT
        *,
        SUM(age) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY country, segment), 4) AS credit_limit_share,
        LEAD(age, 1, 0) OVER w_running AS next_age
    FROM base
    WINDOW w_running AS (ORDER BY credit_limit, id)
),
latest_match AS (
    SELECT
        s.id,
        s.name,
        s.email,
        s.segment,
        s.signup_date,
        s.resolved_segment,
        s.running_age,
        s.credit_limit_share,
        top1.device
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.device
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    SELECT
        *,
        LEAD(credit_limit_share, 1) OVER (PARTITION BY segment ORDER BY running_age, id) AS next_credit_limit_share,
        FIRST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY device ORDER BY credit_limit_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment,
        FIRST_VALUE(device) IGNORE NULLS OVER (ORDER BY running_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_device,
        AVG(running_age) OVER (ORDER BY credit_limit_share, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_age_avg_2,
        FIRST_VALUE(running_age, TRUE) OVER (PARTITION BY device ORDER BY running_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_age
    FROM latest_match
    WHERE running_age >= 120
)
SELECT
    id,
    name,
    resolved_segment,
    running_age,
    credit_limit_share,
    next_credit_limit_share,
    first_known_running_age
FROM metrics;
