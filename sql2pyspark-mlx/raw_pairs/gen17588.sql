WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(country) AS clean_country
    FROM suppliers
    WHERE country = 'TH'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        clean_country,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating) AS rating_percent_rank,
        DENSE_RANK() OVER w_part AS rating_dense_rank,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM valid
    WHERE rating < 4.0
    WINDOW w_part AS (PARTITION BY country ORDER BY rating DESC NULLS FIRST), w_running AS (PARTITION BY city ORDER BY rating, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        rating_dense_rank,
        seq_no,
        LAG(seq_no) OVER (PARTITION BY city ORDER BY rating_dense_rank, id) AS prev_seq_no,
        LEAD(seq_no) OVER (PARTITION BY city ORDER BY rating_dense_rank, id) AS next_seq_no,
        LAG(rating_dense_rank) OVER (PARTITION BY city ORDER BY rating_dense_rank, id) AS prev_rating_dense_rank,
        LEAD(rating_dense_rank) OVER (PARTITION BY city ORDER BY rating_dense_rank, id) AS next_rating_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_dense_rank, id) AS seq_no_2,
        LEAD(seq_no, 1) OVER (ORDER BY seq_no, id) AS next_seq_no_2
    FROM scored
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        prev_seq_no,
        next_rating_dense_rank
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY seq_no DESC, id) = 1
),
enriched_suppliers AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_grp AS next_rating_dense_rank_cume_dist,
        AVG(next_rating_dense_rank) OVER w_part AS city_avg_next_rating_dense_rank,
        MAX(next_rating_dense_rank) OVER w_part AS city_max_next_rating_dense_rank
    FROM ranked
    WINDOW w_grp AS (PARTITION BY city ORDER BY next_rating_dense_rank NULLS FIRST), w_part AS (PARTITION BY city)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched_suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY prev_seq_no DESC, id) = 1
)
SELECT
    id,
    prev_seq_no,
    next_rating_dense_rank,
    next_rating_dense_rank_cume_dist,
    city_avg_next_rating_dense_rank,
    city_max_next_rating_dense_rank
FROM latest;
