WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        AVG(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rating_avg_6
    FROM suppliers
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(rating_avg_5) OVER (PARTITION BY country ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_avg_5,
        AVG(rating_avg_5) OVER (PARTITION BY country ORDER BY rating_share ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rating_avg_5_avg_6_prior
    FROM enriched
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_share ASC, id ASC) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(cumulative_rating_avg_5) OVER w_grp AS country_max_cumulative_rating_avg_5,
        SUM(cumulative_rating_avg_5) OVER (PARTITION BY city ORDER BY cumulative_rating_avg_5 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative_rating_avg_5,
        ROUND(rating_avg_6 / SUM(rating_avg_6) OVER (PARTITION BY city), 4) AS rating_avg_6_share
    FROM latest
    WHERE city IN ('New York', 'Chiang Mai')
    WINDOW w_grp AS (PARTITION BY country)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    city,
    measure,
    metric_value
FROM (
    SELECT
        id,
        city,
        rating_share,
        rating,
        rating_avg_6_share
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (rating_share, rating, rating_avg_6_share)
) u
