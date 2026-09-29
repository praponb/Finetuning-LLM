WITH cleaned AS (
    SELECT *, CASE WHEN country IN ('UK', 'US') THEN 'Group A' ELSE 'Other' END AS country_group
    FROM suppliers
    WHERE supplier_name LIKE 'Lio%'
      AND city IS DISTINCT FROM 'Bangkok'
),
kept AS (
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.cost <= 300
        )
),
metrics AS (
    SELECT
        id,
        supplier_name,
        FIRST_VALUE(city) IGNORE NULLS OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        LEAD(rating, 2) OVER (PARTITION BY country, city ORDER BY rating, id) AS next_rating,
        ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS seq_no,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
        STDDEV_SAMP(rating) OVER (PARTITION BY city) AS city_stddev_samp_rating
    FROM kept
    WHERE supplier_name LIKE 'P%'
),
with_windows AS (
    SELECT
        *,
        MAX(next_rating) OVER (ORDER BY next_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_next_rating,
        AVG(next_rating) OVER (ORDER BY next_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_next_rating,
        SUM(next_rating) OVER (ORDER BY next_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_rating,
        AVG(rating_share) OVER (ORDER BY next_rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rating_share_avg_6_prior
    FROM metrics
)
SELECT
    CASE WHEN rating_share >= 0.84 THEN 'LARGE' ELSE 'SMALL' END AS rating_share_tier,
    seq_no,
    PERCENTILE_APPROX(city_stddev_samp_rating, 0.95) AS city_stddev_samp_rating_p95,
    APPROX_PERCENTILE(rating_share_avg_6_prior, ARRAY(0.25, 0.5, 0.75)) AS rating_share_avg_6_prior_quartiles,
    SUM(running_max_next_rating) AS total_running_max_next_rating,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN rating_share >= 0.84 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM with_windows
GROUP BY rating_share_tier, seq_no
HAVING distinct_ids >= 1
ORDER BY city_stddev_samp_rating_p95 DESC NULLS LAST;
