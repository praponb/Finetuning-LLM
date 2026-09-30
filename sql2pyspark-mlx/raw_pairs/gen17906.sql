WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        ROW_NUMBER() OVER (ORDER BY started_at DESC, id) AS event_seq
    FROM sessions
    WHERE customer_id IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.country,
        b.name,
        b.segment
    FROM scored src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device,
        browser,
        started_at,
        first_known_country,
        country,
        name,
        segment,
        ROW_NUMBER() OVER (PARTITION BY customer_id, browser ORDER BY started_at DESC, id) AS seq_no,
        SUM(event_seq) OVER (PARTITION BY device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS event_seq_sum_7d,
        SUM(event_seq) OVER w_recent AS device_sum_event_seq,
        ROW_NUMBER() OVER w_hist AS event_seq_2,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY segment, browser), 4) AS event_seq_share
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY device, customer_id), w_hist AS (ORDER BY started_at, id)
),
scored_sessions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device,
        started_at,
        name,
        segment,
        seq_no,
        device_sum_event_seq,
        event_seq_2,
        NTILE(3) OVER w_grp AS seq_no_bucket,
        MAX(event_seq_2) OVER w_recent AS event_seq_2_max_30d,
        LAG(device_sum_event_seq, 1, 0) OVER (PARTITION BY country ORDER BY device_sum_event_seq, id) AS prev_device_sum_event_seq,
        PERCENT_RANK() OVER w_grp_2 AS event_seq_share_percent_rank
    FROM metrics
    WINDOW w_grp AS (PARTITION BY country ORDER BY seq_no, id), w_recent AS (PARTITION BY browser, device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY DATE_TRUNC('MONTH', started_at) ORDER BY event_seq_share DESC NULLS LAST)
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.segment,
        s.seq_no,
        s.device_sum_event_seq,
        s.event_seq_2,
        s.event_seq_2_max_30d,
        s.prev_device_sum_event_seq,
        s.event_seq_share_percent_rank,
        top1.web_event_duration_sec
    FROM scored_sessions s
    JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY web_event_duration_sec DESC NULLS FIRST, id) AS rn
        FROM with_latest
    ) x
    WHERE rn = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
        seq_no,
        MAX(event_seq_2) AS max_event_seq_2,
        MAX(event_seq_share_percent_rank) FILTER (WHERE segment = 'SMB') AS smb_max_event_seq_share_percent_rank,
        COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END ORDER BY MAX(event_seq_2) DESC) AS max_event_seq_2_dense_rank
    FROM deduped
    GROUP BY CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END, seq_no
)
SELECT
    seq_no_tier,
    seq_no,
    smb_max_event_seq_share_percent_rank,
    max_event_seq_2_dense_rank
FROM daily;
