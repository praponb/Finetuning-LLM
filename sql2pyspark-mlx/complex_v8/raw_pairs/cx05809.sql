WITH matched AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        rate,
        RANK() OVER (PARTITION BY zone ORDER BY kwh DESC, id) AS kwh_rank,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY zone ORDER BY kwh DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh,
        ROUND(kwh - AVG(kwh) OVER w_read_at_month, 2) AS kwh_vs_read_at_month_avg,
        ROUND(AVG(kwh) OVER (PARTITION BY zone ORDER BY kwh DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS kwh_avg_2_prior
    FROM matched
    WINDOW w_read_at_month AS (PARTITION BY DATE_TRUNC('MONTH', read_at))
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        site_id,
        kwh_rank,
        CASE WHEN second_kwh >= 104.8 THEN 'HIGH' WHEN second_kwh >= 37.0 THEN 'MEDIUM' ELSE 'LOW' END AS second_kwh_tier,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(rate), 2) AS stddev_rate,
        MAX(kwh_rank) AS max_kwh_rank,
        SUM(kwh_rank) AS total_kwh_rank
    FROM windowed
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    site_id,
    second_kwh_tier,
    n_rows,
    stddev_rate,
    max_kwh_rank
FROM grouped
ORDER BY second_kwh_tier
