WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST) AS rating_percent_rank,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share
    FROM suppliers
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        country,
        rating_percent_rank,
        rating_share,
        rating * rating_percent_rank AS rating_times_rating_percent_rank,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM scored
    WHERE country IN ('UK', 'TH', 'DE')
),
scored_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(rating_share) OVER (PARTITION BY country ORDER BY rating_share, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rating_share_avg_3_prior,
        LEAD(rating_times_rating_percent_rank, 1, 0) OVER (PARTITION BY country ORDER BY rating_percent_rank, id) AS next_rating_times_rating_percent_rank,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating_share DESC NULLS LAST) AS rating_share_dense_rank
    FROM cleaned
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        country,
        rating_percent_rank,
        rating_times_rating_percent_rank,
        next_rating_times_rating_percent_rank
    FROM scored_suppliers
) src
UNPIVOT INCLUDE NULLS (
    reading FOR kpi IN (rating_percent_rank, rating_times_rating_percent_rank, next_rating_times_rating_percent_rank)
);
