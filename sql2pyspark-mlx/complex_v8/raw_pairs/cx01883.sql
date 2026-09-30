WITH cleaned_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE kwh < 89.8
      AND voltage <= 234.7
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        quality,
        LEAD(read_at, 2) OVER (ORDER BY kwh ASC NULLS LAST, id) AS next2_read_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY kwh ASC NULLS LAST), 3) AS kwh_percent_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY site_id), 0), 4) AS kwh_share_of_site_id,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        RANK() OVER w_all_by_kwh_ties AS kwh_rank,
        LEAD(read_at, 3) OVER (ORDER BY kwh ASC NULLS LAST, id) AS next3_read_at,
        ROUND(AVG(kwh) OVER (ORDER BY read_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_3,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_id), 2) AS kwh_vs_site_id_avg,
        MIN(kwh) OVER (PARTITION BY site_id) AS site_id_min_kwh,
        LAG(read_at, 3) OVER w_quality_site_id_by_read_at AS prev3_read_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY read_at DESC, id), 3) AS read_at_percent_rank,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_6_prior
    FROM cleaned_readings
    WINDOW w_all_by_kwh_ties AS (ORDER BY kwh ASC NULLS LAST), w_quality_site_id_by_read_at AS (PARTITION BY quality, site_id ORDER BY read_at DESC, id)
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        quality,
        next2_read_at,
        kwh_share_of_site_id,
        next3_read_at,
        kwh_avg_3,
        kwh_vs_site_id_avg,
        site_id_min_kwh,
        prev3_read_at,
        kwh_avg_6_prior,
        MAX(kwh_vs_site_id_avg) OVER (PARTITION BY kwh_rank ORDER BY read_at_percent_rank DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS kwh_vs_site_id_avg_max_4,
        SIZE(COLLECT_SET(quality) OVER w_kwh_rank) AS distinct_quality_in_kwh_rank,
        COUNT(*) OVER (PARTITION BY kwh_rank) AS rows_in_kwh_rank,
        FIRST_VALUE(kwh_avg_3) OVER (PARTITION BY quality ORDER BY kwh_vs_site_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_avg_3,
        MIN(kwh_percent_rank) OVER (PARTITION BY kwh_rank) AS kwh_rank_min_kwh_percent_rank,
        NTH_VALUE(kwh_rank, 2) OVER (PARTITION BY quality ORDER BY kwh_vs_site_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh_rank
    FROM ranked
    WINDOW w_kwh_rank AS (PARTITION BY kwh_rank)
)
-- Final projection
SELECT
    id,
    kwh_share_of_site_id,
    kwh_avg_3,
    kwh_vs_site_id_avg,
    site_id_min_kwh,
    prev3_read_at,
    kwh_avg_6_prior,
    kwh_vs_site_id_avg_max_4,
    distinct_quality_in_kwh_rank,
    rows_in_kwh_rank,
    first_kwh_avg_3,
    kwh_rank_min_kwh_percent_rank,
    second_kwh_rank,
    CASE WHEN kwh_vs_site_id_avg >= 60.0 THEN 'HIGH' WHEN kwh_vs_site_id_avg >= -48.0 THEN 'MEDIUM' ELSE 'LOW' END AS kwh_vs_site_id_avg_tier,
    ROUND(rows_in_kwh_rank / NULLIF(site_id_min_kwh, 0), 3) AS rows_in_kwh_rank_per_site_id_min_kwh
FROM ranked_2
ORDER BY rows_in_kwh_rank_per_site_id_min_kwh, distinct_quality_in_kwh_rank
