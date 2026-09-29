WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        TRIM(country) AS clean_country
    FROM sessions
    WHERE customer_id IS NOT NULL
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM filtered
    EXCEPT
    SELECT customer_id
    FROM sessions
    WHERE TO_DATE(started_at) < '2023-07-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.customer_id = k.customer_id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM verified
    EXCEPT DISTINCT
    SELECT id
    FROM sessions
    WHERE browser IN ('Firefox', 'Edge', 'Safari')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys k ON s.id = k.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.credit_limit,
        d.segment
    FROM reconciled a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(credit_limit) OVER w_part AS browser_min_credit_limit,
        RANK() OVER (ORDER BY credit_limit) AS credit_limit_rank,
        DENSE_RANK() OVER w_grp AS credit_limit_dense_rank
    FROM enriched
    WINDOW w_part AS (PARTITION BY browser), w_grp AS (ORDER BY credit_limit DESC)
),
metrics_sessions AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        clean_country,
        credit_limit,
        segment,
        browser_min_credit_limit,
        credit_limit_rank,
        credit_limit_dense_rank,
        SUM(browser_min_credit_limit) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS browser_min_credit_limit_sum_7d,
        DENSE_RANK() OVER (ORDER BY credit_limit_dense_rank DESC) AS credit_limit_dense_rank_dense_rank
    FROM metrics
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    clean_country,
    credit_limit,
    browser_min_credit_limit,
    credit_limit_rank
FROM metrics_sessions;
