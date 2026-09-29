WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(supplier_name) AS clean_supplier_name,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE city IN ('Bangkok', 'Chiang Mai')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        rating,
        city_clean,
        NTILE(10) OVER (PARTITION BY country ORDER BY rating, id) AS rating_bucket,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM valid
    WHERE country NOT IN ('UK', 'SG')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        rating_bucket,
        running_rating,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating_bucket DESC) AS rating_bucket_dense_rank,
        MIN(rating_bucket) OVER (PARTITION BY country) AS country_min_rating_bucket,
        LAG(rating_bucket, 2) OVER (PARTITION BY country ORDER BY rating_bucket, id) AS prev_rating_bucket
    FROM metrics
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(rating_bucket) OVER w_hist AS next_rating_bucket,
        LEAD(running_rating) OVER w_hist AS next_running_rating,
        ROW_NUMBER() OVER w_hist AS seq_no
    FROM with_windows
    WINDOW w_hist AS (ORDER BY rating_bucket, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        rating_bucket,
        SUM(next_running_rating) OVER w_part AS running_sum_next_running_rating,
        AVG(next_running_rating) OVER w_part AS running_avg_next_running_rating,
        COUNT(next_running_rating) OVER w_part AS running_count_next_running_rating,
        NTILE(4) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket
    FROM scored
    WINDOW w_part AS (ORDER BY prev_rating_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_suppliers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        running_sum_next_running_rating,
        running_avg_next_running_rating,
        running_count_next_running_rating,
        CUME_DIST() OVER (ORDER BY rating_bucket DESC) AS rating_bucket_cume_dist,
        AVG(seq_no_bucket) OVER (ORDER BY running_avg_next_running_rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS seq_no_bucket_avg_2_prior,
        LAST_VALUE(running_avg_next_running_rating) IGNORE NULLS OVER (ORDER BY running_count_next_running_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_avg_next_running_rating,
        DENSE_RANK() OVER (ORDER BY seq_no_bucket DESC NULLS LAST) AS seq_no_bucket_dense_rank
    FROM enriched
)
SELECT
    id,
    running_sum_next_running_rating,
    running_count_next_running_rating,
    seq_no_bucket_avg_2_prior
FROM scored_suppliers
ORDER BY id DESC, running_count_next_running_rating;
