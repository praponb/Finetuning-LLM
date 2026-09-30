WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        DATEDIFF(CURRENT_DATE(), starts_on) AS days_since_starts_on,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM promotions
    WHERE channel IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS latest_rank
    FROM filtered
    QUALIFY latest_rank = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        ends_on
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(channel, TRUE) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS seq_no
    FROM deduped
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        last_known_channel,
        MIN(seq_no) OVER w_part AS last7_min_seq_no,
        AVG(seq_no) OVER w_part AS last7_avg_seq_no,
        MAX(seq_no) OVER w_part AS last7_max_seq_no,
        COUNT(seq_no) OVER w_part AS last7_count_seq_no
    FROM enriched
    WINDOW w_part AS (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last7_max_seq_no >= 7.4 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_max_seq_no_tier,
    last7_count_seq_no,
    AVG(last7_count_seq_no) AS avg_last7_count_seq_no,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(last7_count_seq_no, ARRAY(0.25, 0.5, 0.75)) AS last7_count_seq_no_quartiles
FROM scored
GROUP BY CASE WHEN last7_max_seq_no >= 7.4 THEN 'PREMIUM' ELSE 'STANDARD' END, last7_count_seq_no
