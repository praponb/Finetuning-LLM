WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(age, 0) AS age_rounded,
        COALESCE(segment, 'Unknown') AS segment_filled,
        COALESCE(segment, 'OTHER') AS segment_filled_2
    FROM customers
    WHERE signup_date >= DATE '2023-09-10'
      AND is_active
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY age_rounded DESC) = 1
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.session_device,
        latest.started_at
    FROM deduped s
    JOIN LATERAL (
            SELECT c.device AS session_device, c.started_at
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at <= s.signup_date
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY session_device ORDER BY age_rounded NULLS FIRST) AS age_rounded_rank,
        AVG(age_rounded) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_rounded_avg_5,
        MAX(credit_limit) OVER (PARTITION BY city ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS credit_limit_max_90d,
        MAX(credit_limit) OVER (PARTITION BY city) AS city_max_credit_limit
    FROM latest_match
),
deduped_customers AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_device, segment ORDER BY age_rounded DESC NULLS LAST, id) AS rn
    FROM metrics
    QUALIFY rn = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        segment,
        signup_date,
        credit_limit,
        age_rounded,
        session_device,
        started_at,
        age_rounded_avg_5,
        credit_limit_max_90d,
        rn,
        AVG(credit_limit_max_90d) OVER w_recent AS credit_limit_max_90d_avg_5_prior,
        PERCENT_RANK() OVER (PARTITION BY segment, country ORDER BY age DESC) AS age_percent_rank,
        SUM(age_rounded_avg_5) OVER (PARTITION BY country ORDER BY city_max_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age_rounded_avg_5,
        COUNT(age_rounded_avg_5) OVER w_running AS age_rounded_avg_5_count_7d
    FROM deduped_customers
    WINDOW w_recent AS (ORDER BY age_rounded, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_running AS (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    name,
    segment,
    signup_date,
    credit_limit,
    age_rounded,
    session_device,
    started_at,
    age_rounded_avg_5
FROM enriched
ORDER BY age_rounded_avg_5 DESC NULLS LAST
