WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(kwh - 87.1) AS kwh_distance
    FROM meter_readings
    WHERE quality = 'EST'
      AND site_id IS NOT NULL
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.kwh_distance,
        si.tariff AS site_tariff
    FROM recent s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff IN ('EV', 'FLAT')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY site_tariff, site_id ORDER BY kwh_distance, id) AS rn,
        MAX(kwh_distance) OVER (PARTITION BY site_id) AS site_id_max_kwh_distance,
        LAST_VALUE(kwh) OVER (PARTITION BY site_tariff, site_id ORDER BY kwh_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_kwh,
        LEAD(kwh_distance) OVER (PARTITION BY site_id, site_tariff ORDER BY kwh_distance DESC NULLS FIRST, id) AS next_kwh_distance,
        ROUND(kwh_distance / NULLIF(SUM(kwh_distance) OVER w_site_id, 0), 4) AS kwh_distance_share_of_site_id,
        MIN(kwh_distance) OVER w_site_id AS site_id_min_kwh_distance,
        DENSE_RANK() OVER (PARTITION BY site_id, site_tariff ORDER BY kwh_distance DESC NULLS FIRST) AS kwh_distance_dense_rank
    FROM with_parent
    WINDOW w_site_id AS (PARTITION BY site_id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN site_tariff IN ('FLAT', 'TOU') THEN 'group_a' WHEN site_tariff = 'EV' THEN 'group_b' ELSE 'other' END AS site_tariff_group,
        CASE WHEN site_tariff IN ('FLAT', 'TOU') THEN 'group_a' WHEN site_tariff = 'EV' THEN 'group_b' ELSE 'other' END AS site_tariff_group_2,
        LOWER(TRIM(site_tariff)) AS site_tariff_clean
    FROM metrics
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY rn, site_tariff_group) AS rows_in_rn_site_tariff_group,
        FIRST_VALUE(kwh_distance_dense_rank) OVER (ORDER BY next_kwh_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh_distance_dense_rank,
        SUM(last_kwh) OVER (PARTITION BY rn, site_tariff_group) AS rn_site_tariff_group_sum_last_kwh,
        kwh_distance - LAG(kwh_distance) OVER (ORDER BY next_kwh_distance DESC, id) AS kwh_distance_change,
        MAX(kwh_distance) OVER (ORDER BY next_kwh_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_distance_so_far,
        ROUND(CUME_DIST() OVER w_rn_site_id_by_next_kwh_dista, 3) AS next_kwh_distance_cume_dist
    FROM derived
    WINDOW w_rn_site_id_by_next_kwh_dista AS (PARTITION BY rn, site_id ORDER BY next_kwh_distance)
)
SELECT
    id,
    site_id,
    kwh,
    kwh_distance,
    site_tariff,
    site_id_max_kwh_distance,
    last_kwh,
    next_kwh_distance,
    kwh_distance_share_of_site_id,
    site_id_min_kwh_distance,
    kwh_distance_dense_rank,
    site_tariff_group,
    site_tariff_group_2,
    site_tariff_clean,
    rows_in_rn_site_tariff_group,
    first_kwh_distance_dense_rank,
    rn_site_tariff_group_sum_last_kwh,
    max_kwh_distance_so_far,
    ROUND(kwh_distance_dense_rank / NULLIF(max_kwh_distance_so_far, 0), 3) AS kwh_distance_dense_rank_per_max_kwh_distance_so_,
    LOWER(REGEXP_REPLACE(TRIM(site_tariff_group), ' ', '_')) AS site_tariff_group_slug
FROM sequenced
ORDER BY site_tariff_group_2 NULLS LAST
