WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CASE WHEN rating >= 3.8 THEN 'High' WHEN rating >= 2.4 THEN 'Medium' ELSE 'Low' END rating_tier,
        UPPER(supplier_name) supplier_name_upper,
        COALESCE(city, 'UNKNOWN') city_clean
    FROM suppliers
    WHERE rating BETWEEN 3.2 AND 4.2
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'eco')
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        supplier_name_upper,
        city_clean,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating DESC, id) row_seq,
        PERCENT_RANK() OVER (PARTITION BY country, city ORDER BY rating NULLS LAST) rating_percent_rank
    FROM not_flagged
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(rating_percent_rank) IGNORE NULLS OVER w_part last_known_rating_percent_rank,
        FIRST_VALUE(row_seq, TRUE) OVER (ORDER BY rating_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_row_seq,
        CUME_DIST() OVER (PARTITION BY country ORDER BY row_seq DESC NULLS FIRST) row_seq_cume_dist
    FROM metrics
    WINDOW w_part AS (PARTITION BY country ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END AS first_known_row_seq_tier,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    SUM(CASE WHEN country = 'US' THEN row_seq_cume_dist ELSE 0 END) AS us_row_seq_cume_dist,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    AVG(rating_percent_rank) FILTER (WHERE country = 'UK') AS uk_avg_rating_percent_rank,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
    AVG(last_known_rating_percent_rank) FILTER (WHERE country = 'US') AS us_avg_last_known_rating_percent_rank
FROM enriched
GROUP BY CASE WHEN first_known_row_seq >= 9 THEN 'LARGE' ELSE 'SMALL' END
