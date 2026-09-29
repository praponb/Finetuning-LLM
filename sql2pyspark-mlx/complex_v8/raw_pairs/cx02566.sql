WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CONCAT_WS('|', device, source) AS device_source_label,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-19 13:00:00'
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.source,
        s.played_at_label,
        l.country AS listener_country
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        source,
        played_at_label,
        listener_country,
        RANK() OVER (PARTITION BY track_id, listener_country ORDER BY played_at ASC, id) AS played_at_rank,
        DENSE_RANK() OVER (PARTITION BY track_id, listener_country ORDER BY played_at ASC) AS played_at_dense_rank,
        ROUND(CUME_DIST() OVER w_played_at_year_listener_id_b, 3) AS played_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY YEAR(played_at), listener_id ORDER BY played_at ASC, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY YEAR(played_at), listener_id ORDER BY played_at ASC) AS played_at_dense_rank_2
    FROM combined_details_listener
    WINDOW w_played_at_year_listener_id_b AS (PARTITION BY YEAR(played_at), listener_id ORDER BY played_at ASC)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        source,
        played_at_label,
        listener_country,
        played_at_rank,
        seq_no,
        SUM(played_at_dense_rank) OVER w_played_at_dense_rank_listene AS played_at_dense_rank_listener_id_sum_played_at_d,
        SIZE(COLLECT_SET(listener_country) OVER (PARTITION BY played_at_dense_rank, listener_id)) AS distinct_listener_country_in_played_at_dense_ran,
        MAX(played_at_dense_rank) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at_dense_rank_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_played_at_dense_rank_so_far,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY played_at_dense_rank, listener_id)) AS distinct_source_in_played_at_dense_rank_listener
    FROM sequenced
    WINDOW w_played_at_dense_rank_listene AS (PARTITION BY played_at_dense_rank, listener_id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        played_at,
        source,
        played_at_label,
        listener_country,
        played_at_rank,
        seq_no,
        played_at_dense_rank_listener_id_sum_played_at_d,
        max_played_at_dense_rank_so_far,
        distinct_source_in_played_at_dense_rank_listener,
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        played_at_rank - seq_no AS played_at_rank_minus_seq_no
    FROM sequenced_2
)
SELECT
    id,
    listener_id,
    played_at,
    source,
    listener_country,
    played_at_dense_rank_listener_id_sum_played_at_d,
    max_played_at_dense_rank_so_far,
    distinct_source_in_played_at_dense_rank_listener,
    source_prefix,
    played_at_rank_minus_seq_no,
    seq_no + played_at_rank AS seq_no_plus_played_at_rank
FROM annotated
ORDER BY played_at_rank_minus_seq_no, played_at
