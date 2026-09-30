WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC NULLS FIRST, id) AS latest_rank
        FROM suppliers
    ) x
    WHERE latest_rank = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        latest_rank,
        ROUND(latest_rank / rating, 2) AS latest_rank_per_rating
    FROM latest
    WHERE latest_rank < 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(latest_rank_per_rating, 1) OVER (ORDER BY latest_rank_per_rating, id) AS next_latest_rank_per_rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY latest_rank DESC, id) AS seq_no,
        SUM(latest_rank_per_rating) OVER w_part AS cumulative_latest_rank_per_rating,
        AVG(latest_rank_per_rating) OVER (ORDER BY latest_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS latest_rank_per_rating_avg_3
    FROM cleaned
    WINDOW w_part AS (PARTITION BY country, city ORDER BY latest_rank_per_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY latest_rank_per_rating DESC NULLS LAST, id) AS rn
    FROM metrics
    QUALIFY rn = 1
)
SELECT
    id,
    supplier_name,
    city,
    latest_rank,
    latest_rank_per_rating,
    next_latest_rank_per_rating,
    seq_no,
    cumulative_latest_rank_per_rating,
    latest_rank_per_rating_avg_3,
    CASE WHEN supplier_name RLIKE '^(Siam Parts Co|Pacific Wholesale)$' THEN 'CORE' ELSE 'EXTENDED' END AS supplier_name_zone
FROM deduped
ORDER BY cumulative_latest_rank_per_rating DESC
