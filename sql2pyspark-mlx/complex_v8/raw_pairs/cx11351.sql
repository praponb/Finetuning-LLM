WITH valid_plays AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE source = 'album'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY is_skipped, device ORDER BY played_at DESC, id) AS rn,
        SUM(ms_played) OVER (PARTITION BY is_skipped, device ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_sum_6_prior,
        DENSE_RANK() OVER (ORDER BY ms_played DESC NULLS FIRST) AS ms_played_dense_rank
    FROM valid_plays
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at
    FROM ranked s
)
SELECT
    ms_played_dense_rank,
    rn,
    ROUND(SUM(ms_played_dense_rank) / NULLIF(SUM(ms_played_sum_6_prior), 0), 3) AS ms_played_dense_rank_to_ms_played_sum_6_prior_ra
FROM profiled
GROUP BY ROLLUP(ms_played_dense_rank, rn)
ORDER BY ms_played_dense_rank NULLS LAST, rn NULLS LAST
