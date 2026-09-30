WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(browser, 'N/A') AS browser_filled,
        SUBSTRING(country, 1, 2) AS country_prefix
    FROM sessions
    WHERE device = 'desktop'
      AND customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.device,
        e.browser,
        e.country,
        e.browser_filled,
        c.signup_date,
        c.email,
        c.name
    FROM base e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) AS seq_no,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER w_ordered AS event_seq_2
    FROM joined
    WINDOW w_grp AS (PARTITION BY browser ORDER BY signup_date DESC, id), w_ordered AS (PARTITION BY device ORDER BY signup_date DESC, id)
),
enriched_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(seq_no) OVER w_part AS running_sum_seq_no,
        AVG(seq_no) OVER w_part AS running_avg_seq_no,
        COUNT(seq_no) OVER w_part AS running_count_seq_no,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY customer_id), 4) AS seq_no_share
    FROM enriched
    WHERE event_seq = 8
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    running_count_seq_no,
    APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    MIN(event_seq) AS min_event_seq,
    APPROX_PERCENTILE(seq_no, 0.5) AS seq_no_p50
FROM enriched_sessions
GROUP BY running_count_seq_no
HAVING seq_no_p50 > 0;
