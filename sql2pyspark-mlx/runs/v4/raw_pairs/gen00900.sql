WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        HOUR(started_at) AS started_at_hour,
        COALESCE(browser, 'N/A') AS browser_filled
    FROM sessions
    WHERE started_at >= DATE '2023-10-16'
      AND country IS NOT NULL
      AND device NOT IN ('tablet', 'desktop', 'mobile')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        started_at_hour,
        browser_filled,
        ROW_NUMBER() OVER w_part AS seq_no,
        COUNT(started_at_hour) OVER w_recent AS started_at_hour_count_90d
    FROM valid
    WINDOW w_part AS (PARTITION BY country ORDER BY started_at DESC, id), w_recent AS (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        started_at_hour,
        browser_filled,
        seq_no,
        started_at_hour_count_90d,
        LAG(started_at_hour_count_90d, 1) OVER (ORDER BY started_at_hour, id) AS prev_started_at_hour_count_90d,
        ROW_NUMBER() OVER w_part AS row_seq,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at_hour_count_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        COUNT(started_at_hour) OVER (PARTITION BY customer_id, browser ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS started_at_hour_count_90d_2,
        ROUND(started_at_hour_count_90d / SUM(started_at_hour_count_90d) OVER (PARTITION BY browser), 4) AS started_at_hour_count_90d_share
    FROM metrics
    WINDOW w_part AS (PARTITION BY country ORDER BY started_at_hour DESC, id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY prev_started_at_hour_count_90d DESC NULLS LAST, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, d.signup_date
    FROM ranked src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered AS started_at_hour_percent_rank,
        LAG(row_seq, 2) OVER (ORDER BY signup_date, id) AS prev_row_seq
    FROM joined
    WINDOW w_ordered AS (PARTITION BY browser ORDER BY started_at_hour DESC)
),
stats AS (
    -- Grouped metrics
    SELECT
        seq_no,
        SUM(CASE WHEN browser = 'Firefox' THEN seq_no ELSE 0 END) AS firefox_seq_no,
        ROUND(SUM(started_at_hour_percent_rank) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS started_at_hour_percent_rank_per_sg,
        GROUPING(seq_no) AS grouping_id
    FROM with_windows
    GROUP BY CUBE (seq_no)
    HAVING firefox_seq_no > 0
)
SELECT
    seq_no,
    started_at_hour_percent_rank_per_sg,
    grouping_id
FROM stats
ORDER BY seq_no DESC
