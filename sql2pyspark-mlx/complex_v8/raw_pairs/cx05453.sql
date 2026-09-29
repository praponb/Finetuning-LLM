WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'focus')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date DESC, id), 3) AS release_date_percent_rank,
        NTILE(10) OVER w_artist_id_is_explicit_by_rel AS release_date_decile,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id), 3) AS release_date_cume_dist,
        RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id) AS release_date_rank,
        NTILE(5) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id) AS release_date_quintile,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id) AS seq_no,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROW_NUMBER() OVER (ORDER BY release_date, id) AS release_date_seq,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id)) AS days_since_prev_release_date,
        RANK() OVER (ORDER BY release_date, id) AS release_date_rank_2,
        LEAD(release_date, 3) OVER w_artist_id_by_release_date AS next3_release_date,
        ROW_NUMBER() OVER (ORDER BY release_date DESC, id) AS rn
    FROM valid
    WINDOW w_artist_id_is_explicit_by_rel AS (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC, id), w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC, id)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        release_date_percent_rank,
        release_date_decile,
        last_known_release_date,
        release_date_cume_dist,
        release_date_quintile,
        seq_no,
        release_date_rank_2,
        next3_release_date,
        release_date_cume_dist - LAG(release_date_cume_dist) OVER w_release_date_quintile_last_k AS release_date_cume_dist_change,
        ROUND(CUME_DIST() OVER (PARTITION BY release_date_quintile, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY release_date_seq NULLS LAST), 3) AS release_date_seq_cume_dist,
        ROUND(AVG(seq_no) OVER (PARTITION BY release_date_quintile, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY release_date_seq NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS seq_no_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY release_date_quintile, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY release_date_seq NULLS LAST), 3) AS release_date_seq_percent_rank,
        MAX(release_date_decile) OVER w_release_date_rank_2 AS release_date_rank_2_max_release_date_decile,
        ROUND(PERCENT_RANK() OVER w_release_date_seq_by_rn_ties, 3) AS rn_percent_rank,
        NTH_VALUE(release_date_seq, 2) OVER (PARTITION BY release_date_quintile, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY release_date_seq NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_seq,
        MAX(release_date_rank) OVER (PARTITION BY release_date_seq ORDER BY rn ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS release_date_rank_max_2
    FROM with_windows
    WINDOW w_release_date_quintile_last_k AS (PARTITION BY release_date_quintile, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY release_date_seq NULLS LAST, id), w_release_date_rank_2 AS (PARTITION BY release_date_rank_2), w_release_date_seq_by_rn_ties AS (PARTITION BY release_date_seq ORDER BY rn ASC)
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        PERCENTILE_APPROX(release_date_rank_2, 0.5) AS median_release_date_rank_2,
        MAX(release_date_rank_2) AS max_release_date_rank_2,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false,
        ROUND(AVG(release_date_percent_rank), 2) AS avg_release_date_percent_rank
    FROM ranked
    GROUP BY DATE_FORMAT(release_date, 'yyyy-MM')
)
-- Final projection
SELECT
    release_date_ym,
    median_release_date_rank_2,
    max_release_date_rank_2,
    avg_release_date_percent_rank
FROM grouped
ORDER BY median_release_date_rank_2 DESC NULLS FIRST
