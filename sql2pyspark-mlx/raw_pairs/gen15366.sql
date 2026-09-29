WITH enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.plan,
        s.monthly_fee,
        s.status,
        p.country,
        p.city,
        p.credit_limit
    FROM subscriptions s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY credit_limit DESC, id) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        country,
        credit_limit
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY credit_limit DESC NULLS FIRST, id) AS row_num
        FROM latest
    ) t
    WHERE row_num = 1
),
enriched_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(credit_limit, 1) OVER (ORDER BY credit_limit, id) AS prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS row_seq
    FROM ranked
),
latest_subscriptions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY prev_credit_limit NULLS LAST, id) AS rank_in_group
    FROM enriched_subscriptions
    QUALIFY rank_in_group = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY prev_credit_limit DESC, id) AS dedup_rank
        FROM latest_subscriptions
    ) x
    WHERE dedup_rank = 1
),
deduped_subscriptions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        prev_credit_limit,
        row_seq,
        dedup_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY row_seq NULLS FIRST, id) AS latest_rank
        FROM deduped
    ) x
    WHERE latest_rank = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    PERCENTILE_APPROX(prev_credit_limit, 0.9) AS prev_credit_limit_p90,
    ROUND(AVG(row_seq), 2) AS avg_row_seq_2
FROM deduped_subscriptions
GROUP BY CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY avg_row_seq_2 DESC
