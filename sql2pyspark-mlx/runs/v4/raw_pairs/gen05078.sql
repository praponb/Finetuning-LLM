WITH filtered AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        LENGTH(supplier_name) AS supplier_name_length,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND country <> 'UK'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
enriched AS (
    SELECT
        *,
        LAG(supplier_name_length, 1) OVER (ORDER BY rating ASC, id ASC) AS prev_supplier_name_length,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY rating DESC NULLS LAST) AS rating_percent_rank,
        NTILE(5) OVER (PARTITION BY city ORDER BY supplier_name_length ASC, id ASC) AS supplier_name_length_bucket
    FROM latest
),
scored AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        supplier_name_length,
        resolved_city,
        dedup_rank,
        prev_supplier_name_length,
        rating_percent_rank,
        supplier_name_length_bucket,
        NTILE(3) OVER (PARTITION BY city ORDER BY supplier_name_length ASC, id ASC) AS supplier_name_length_bucket_2,
        NTILE(10) OVER (PARTITION BY city ORDER BY supplier_name_length_bucket DESC, id ASC) AS supplier_name_length_bucket_bucket,
        SUM(rating_percent_rank) OVER (PARTITION BY city ORDER BY supplier_name_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_percent_rank
    FROM enriched
)
SELECT
    CASE WHEN rating_percent_rank >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_percent_rank_tier,
    COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'London' THEN 1 END), 0), 2) AS rating_per_london,
    MAX(running_rating_percent_rank) AS max_running_rating_percent_rank,
    SUM(supplier_name_length_bucket_2) FILTER (WHERE city = 'Tokyo') AS tokyo_total_supplier_name_length_bucket_2
FROM scored
GROUP BY 1
HAVING tokyo_total_supplier_name_length_bucket_2 > 0;
