WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', status, plan) AS status_plan_key,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE start_date > TO_DATE('2023-08-04')
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        plan,
        monthly_fee,
        days_since_end_date,
        AVG(days_since_end_date) OVER (PARTITION BY customer_id ORDER BY days_since_end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_days_since_end_date,
        SUM(days_since_end_date) OVER (PARTITION BY customer_id ORDER BY days_since_end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_days_since_end_date,
        MIN(days_since_end_date) OVER (PARTITION BY customer_id ORDER BY days_since_end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_days_since_end_date,
        MAX(days_since_end_date) OVER (PARTITION BY customer_id ORDER BY days_since_end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_days_since_end_date
    FROM filtered
    WHERE customer_id IN (2, 19, 25)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY last7_max_days_since_end_date DESC, id) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.days_since_end_date,
        a.last7_sum_days_since_end_date,
        a.dedup_rank,
        c.age
    FROM ranked a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.segment,
        d.country,
        d.name
    FROM joined s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY country ORDER BY age, id) AS age_bucket,
        FIRST_VALUE(last7_sum_days_since_end_date, TRUE) OVER (PARTITION BY plan ORDER BY dedup_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last7_sum_days_since_end_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY dedup_rank, id) AS row_seq
    FROM enriched_subscriptions
),
enriched_subscriptions_2 AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.age AS customer_age,
        b.city,
        b.signup_date
    FROM metrics src
    INNER JOIN customers b
        ON src.customer_id = b.id
)
-- Join a dimension and aggregate
SELECT
    c.segment AS customer_segment,
    e.city AS subscription_city,
    COUNT(*) AS rows_total,
    SUM(e.days_since_end_date) AS total_days_since_end_date,
    ROUND(SUM(e.row_seq), 2) AS total_row_seq,
    GROUPING_ID() AS gid
FROM enriched_subscriptions_2 e
INNER JOIN customers c ON e.customer_id = c.id
GROUP BY ROLLUP (c.segment, e.city)
ORDER BY total_row_seq DESC NULLS FIRST;
