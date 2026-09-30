WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        CASE WHEN bpm >= 127 THEN 'HIGH' WHEN bpm >= 82 THEN 'MEDIUM' ELSE 'LOW' END AS bpm_tier
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-09-26' AND DATE '2025-11-14'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        DATE_TRUNC('MONTH', release_date_month) AS release_date_month_month,
        ROUND(100 * COUNT(CASE WHEN NOT is_explicit THEN 1 END) / COUNT(*), 1) AS pct_not_is_explicit,
        COUNT(DISTINCT bpm_tier) AS distinct_bpm_tiers,
        SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true,
        ROUND(100 * COUNT(CASE WHEN is_explicit = TRUE THEN 1 END) / COUNT(*), 1) AS pct_is_explicit_true
    FROM scoped
    WHERE is_explicit_label IS NOT NULL
    GROUP BY 1, 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY artist_id, n_is_explicit_true ORDER BY distinct_bpm_tiers DESC, artist_id, release_date_month_month) AS distinct_bpm_tiers_tercile,
        LAG(distinct_bpm_tiers) OVER w_artist_id_n_is_explicit_true AS prev_distinct_bpm_tiers,
        ROUND(PERCENT_RANK() OVER w_all_by_n_is_explicit_true_ti, 3) AS n_is_explicit_true_percent_rank,
        ROUND(n_is_explicit_true / NULLIF(SUM(n_is_explicit_true) OVER w_distinct_bpm_tiers, 0), 4) AS n_is_explicit_true_share_of_distinct_bpm_tiers
    FROM totals
    WINDOW w_artist_id_n_is_explicit_true AS (PARTITION BY artist_id, n_is_explicit_true ORDER BY distinct_bpm_tiers DESC, artist_id, release_date_month_month), w_all_by_n_is_explicit_true_ti AS (ORDER BY n_is_explicit_true), w_distinct_bpm_tiers AS (PARTITION BY distinct_bpm_tiers)
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.artist_id,
        s.release_date_month_month,
        s.pct_not_is_explicit,
        s.distinct_bpm_tiers,
        s.n_is_explicit_true,
        s.pct_is_explicit_true,
        s.prev_distinct_bpm_tiers,
        s.n_is_explicit_true_percent_rank,
        s.n_is_explicit_true_share_of_distinct_bpm_tiers,
        a.country AS artist_country
    FROM windowed s
    JOIN artists a ON a.id = s.artist_id
)
SELECT artist_id, release_date_month_month, field, amount
FROM (
    SELECT artist_id, release_date_month_month, n_is_explicit_true_percent_rank, n_is_explicit_true_share_of_distinct_bpm_tiers, pct_is_explicit_true
    FROM with_details
) src
UNPIVOT (
    amount FOR field IN (n_is_explicit_true_percent_rank, n_is_explicit_true_share_of_distinct_bpm_tiers, pct_is_explicit_true)
) unp
