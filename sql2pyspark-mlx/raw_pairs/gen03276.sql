WITH joined AS (
    SELECT
        e.*,
        s.sqft,
        s.opened_date,
        s.city
    FROM store_sales e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS rn
        FROM joined
    ) x
    WHERE rn = 1
),
deduped AS (
    SELECT
        id,
        store_id,
        sqft,
        opened_date
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY rn) AS latest_rank
        FROM ranked
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist,
        SUM(sqft) OVER (PARTITION BY store_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        RANK() OVER (PARTITION BY store_id ORDER BY sqft DESC) AS sqft_rank,
        SUM(sqft) OVER (PARTITION BY DATE_TRUNC('week', opened_date), store_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_2
    FROM deduped
),
ranked_store_sales AS (
    SELECT
        id,
        sqft,
        opened_date,
        cumulative_sqft,
        sqft_rank,
        cumulative_sqft_2
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sqft DESC NULLS LAST, id) = 1
),
metrics AS (
    SELECT
        *,
        SUM(sqft) OVER w_recent AS last3_sum_sqft,
        MIN(sqft) OVER w_recent AS last3_min_sqft,
        AVG(sqft) OVER w_recent AS last3_avg_sqft,
        MIN(cumulative_sqft) OVER w_ordered AS opened_date_month_min_cumulative_sqft
    FROM ranked_store_sales
    WHERE cumulative_sqft_2 >= 76200
    WINDOW w_recent AS (ORDER BY cumulative_sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY MONTH(opened_date))
),
with_windows AS (
    SELECT
        *,
        COUNT(last3_avg_sqft) OVER (ORDER BY last3_avg_sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_last3_avg_sqft,
        MIN(last3_avg_sqft) OVER (ORDER BY last3_avg_sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_last3_avg_sqft,
        MAX(last3_avg_sqft) OVER (ORDER BY last3_avg_sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_last3_avg_sqft,
        AVG(last3_avg_sqft) OVER (ORDER BY last3_avg_sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_last3_avg_sqft
    FROM metrics
)
SELECT
    CASE WHEN cumulative_sqft >= 87934 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_sqft_tier,
    APPROX_PERCENTILE(cumulative_sqft, 0.95) AS cumulative_sqft_p95,
    APPROX_PERCENTILE(last3_min_sqft, ARRAY(0.25, 0.5, 0.75)) AS last3_min_sqft_quartiles,
    APPROX_PERCENTILE(last4_max_last3_avg_sqft, 0.5) AS last4_max_last3_avg_sqft_p50,
    AVG(last4_min_last3_avg_sqft) AS avg_last4_min_last3_avg_sqft,
    COUNT(*) AS cnt,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY CUBE (CASE WHEN cumulative_sqft >= 87934 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY cnt DESC NULLS FIRST
