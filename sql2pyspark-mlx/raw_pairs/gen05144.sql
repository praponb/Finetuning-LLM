WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, WEEKOFYEAR(started_at) AS started_at_weekofyear
    FROM sessions
    WHERE country IS DISTINCT FROM 'SG'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(started_at_weekofyear) OVER (PARTITION BY browser, device ORDER BY started_at, id) AS next_started_at_weekofyear,
        ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at, id) AS row_seq,
        SUM(started_at_weekofyear) OVER (PARTITION BY device ORDER BY started_at_weekofyear, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_started_at_weekofyear
    FROM filtered
    WHERE country = 'UK'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.web_event_event_time,
        x.web_event_event_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        country,
        started_at_weekofyear,
        next_started_at_weekofyear,
        row_seq,
        cumulative_started_at_weekofyear,
        web_event_event_time,
        web_event_event_type,
        MAX(cumulative_started_at_weekofyear) OVER (PARTITION BY MONTH(started_at), browser) AS started_at_month_max_cumulative_started_at_weekofyear,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY device, web_event_event_type), 4) AS row_seq_share,
        LEAD(row_seq, 2, 0) OVER (PARTITION BY browser ORDER BY started_at_weekofyear, id) AS next_row_seq
    FROM with_last
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, web_event_event_type ORDER BY next_started_at_weekofyear DESC, id) AS latest_rank
    FROM enriched_sessions
    QUALIFY latest_rank = 1
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        device,
        PERCENTILE_APPROX(started_at_month_max_cumulative_started_at_weekofyear, ARRAY(0.25, 0.5, 0.75)) AS started_at_month_max_cumulative_started_at_weekofyear_quartiles,
        SUM(CASE WHEN country = 'SG' THEN row_seq_share ELSE 0 END) AS sg_row_seq_share,
        SIZE(COLLECT_SET(country)) AS num_countries
    FROM latest
    GROUP BY 1
)
SELECT
    device,
    started_at_month_max_cumulative_started_at_weekofyear_quartiles,
    num_countries
FROM grouped
ORDER BY device NULLS LAST
