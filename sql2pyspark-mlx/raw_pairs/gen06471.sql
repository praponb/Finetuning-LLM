WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        rating,
        LEAD(rating) OVER w_running AS next_rating,
        ROW_NUMBER() OVER w_running AS event_seq,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM suppliers
    WINDOW w_running AS (PARTITION BY city ORDER BY rating ASC, id ASC)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(rating) OVER (PARTITION BY country ORDER BY event_seq ASC, id ASC) AS next_rating_2,
        LAG(rating) OVER (PARTITION BY country ORDER BY event_seq ASC, id ASC) AS prev_rating,
        LAG(running_rating) OVER (PARTITION BY country ORDER BY event_seq ASC, id ASC) AS prev_running_rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY event_seq ASC, id ASC) AS row_seq,
        NTILE(10) OVER (PARTITION BY country ORDER BY running_rating ASC, id ASC) AS running_rating_bucket
    FROM scored
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        rating,
        event_seq,
        next_rating_2,
        prev_rating,
        prev_running_rating,
        row_seq,
        running_rating_bucket,
        next_rating - prev_rating AS next_rating_minus_prev_rating,
        GREATEST(prev_rating, running_rating) AS greatest_prev_rating_running_rating
    FROM enriched
    WHERE prev_running_rating BETWEEN 8 AND 18
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
),
scored_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(next_rating_minus_prev_rating) OVER (PARTITION BY country ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_rating_minus_prev_rating,
        MIN(next_rating_minus_prev_rating) OVER (PARTITION BY country ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_next_rating_minus_prev_rating,
        COUNT(next_rating_minus_prev_rating) OVER (PARTITION BY country ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_next_rating_minus_prev_rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY prev_rating ASC, id ASC) AS event_seq_2
    FROM not_flagged
    WHERE prev_running_rating BETWEEN 13 AND 17
)
-- Multi-level aggregation
SELECT
    CASE WHEN event_seq_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_2_tier,
    CASE WHEN prev_rating >= 3.9 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_rating_tier,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT_IF(country = 'TH') AS th_events,
    SUM(row_seq) FILTER (WHERE country = 'UK') AS uk_total_row_seq,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    APPROX_PERCENTILE(next_rating_minus_prev_rating, ARRAY(0.25, 0.5, 0.75)) AS next_rating_minus_prev_rating_quartiles,
    ROUND(SUM(running_sum_next_rating_minus_prev_rating) / NULLIF(COUNT(CASE WHEN country = 'JP' THEN 1 END), 0), 2) AS running_sum_next_rating_minus_prev_rating_per_jp
FROM scored_suppliers
GROUP BY CASE WHEN event_seq_2 >= 9 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN prev_rating >= 3.9 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING running_sum_next_rating_minus_prev_rating_per_jp >= 0
ORDER BY uk_total_row_seq DESC
