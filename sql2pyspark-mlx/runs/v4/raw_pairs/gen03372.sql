WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(rating) AS rating_whole
    FROM suppliers
    WHERE rating >= 1.9
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY country ORDER BY rating NULLS FIRST) AS rating_rank,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_dense_rank,
        AVG(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_avg_3,
        SUM(rating_whole) OVER (PARTITION BY city, country ORDER BY rating_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_whole,
        ROW_NUMBER() OVER (ORDER BY rating DESC, id) AS seq_no
    FROM filtered
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        country,
        city,
        rating,
        rating_whole,
        rating_rank,
        rating_dense_rank,
        rating_avg_3,
        seq_no,
        rating_rank / NULLIF(rating_whole, 0) AS rating_rank_per_rating_whole,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM enriched
    WHERE rating_avg_3 > 3
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating_dense_rank NULLS LAST, id) AS latest_rank
        FROM cleaned
    ) t
    WHERE latest_rank = 1
)
-- Ranking and gap features
SELECT
    id,
    country,
    city,
    rating,
    rating_whole,
    rating_rank,
    rating_dense_rank,
    rating_avg_3,
    seq_no,
    rating_rank_per_rating_whole,
    resolved_city,
    latest_rank,
    LEAD(rating_avg_3, 1) OVER (PARTITION BY city, country ORDER BY rating_rank, id) AS next_rating_avg_3,
    ROUND(rating_rank / SUM(rating_rank) OVER (PARTITION BY country), 4) AS rating_rank_share,
    LEAD(rating_whole, 1) OVER (ORDER BY seq_no, id) AS next_rating_whole,
    AVG(seq_no) OVER w_part AS seq_no_avg_3
FROM latest
WINDOW w_part AS (PARTITION BY city ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
