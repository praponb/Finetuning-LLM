WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE product = 'TRAVEL'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        premium,
        status,
        COUNT(*) OVER (PARTITION BY product, YEAR(start_date)) AS rows_in_product_start_date_year,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium DESC, id) AS rn,
        NTILE(10) OVER (PARTITION BY product ORDER BY premium DESC, id) AS premium_decile,
        SUM(premium) OVER (PARTITION BY product, YEAR(start_date)) AS product_start_date_year_sum_premium,
        ROUND(CUME_DIST() OVER w_product_by_premium_ties, 3) AS premium_cume_dist,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product, YEAR(start_date)), 0), 4) AS premium_share_of_product_start_date_year
    FROM cleaned
    WINDOW w_product_by_premium_ties AS (PARTITION BY product ORDER BY premium DESC)
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM windowed s
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM with_stats
    WHERE premium_cume_dist < (SELECT MAX(premium_cume_dist) FROM with_stats)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        product,
        premium,
        rows_in_product_start_date_year,
        rn,
        product_start_date_year_sum_premium,
        premium_cume_dist,
        claim_count,
        COUNT(*) OVER (PARTITION BY product, premium_decile) AS rows_in_product_premium_decile,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY product, premium_decile), 0), 4) AS claim_count_share_of_product_premium_decile,
        NTILE(4) OVER (PARTITION BY product ORDER BY rn NULLS LAST, id) AS rn_quartile,
        FIRST_VALUE(premium_cume_dist) IGNORE NULLS OVER (PARTITION BY rn ORDER BY premium_share_of_product_start_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY rn NULLS LAST, id) AS seq_no,
        FIRST_VALUE(premium_cume_dist) OVER (PARTITION BY product ORDER BY rn NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium_cume_dist
    FROM screened
),
totals AS (
    -- Group-level totals
    SELECT
        product,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(first_known_premium_cume_dist, 0.5) AS median_first_known_premium_cume_dist,
        SUM(rows_in_product_premium_decile) AS total_rows_in_product_premium_decile,
        ROUND(SUM(first_known_premium_cume_dist) / NULLIF(SUM(seq_no), 0), 3) AS first_known_premium_cume_dist_to_seq_no_ratio
    FROM metrics
    GROUP BY product
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    product,
    n_rows,
    median_first_known_premium_cume_dist,
    total_rows_in_product_premium_decile,
    first_known_premium_cume_dist_to_seq_no_ratio
FROM totals
WHERE total_rows_in_product_premium_decile > 19
ORDER BY total_rows_in_product_premium_decile DESC, median_first_known_premium_cume_dist DESC
