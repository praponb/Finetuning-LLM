WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        UPPER(TRIM(product)) AS product_clean
    FROM policies
    WHERE product = 'AUTO'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_product_by_premium, 3) AS premium_percent_rank,
        ROUND(premium - AVG(premium) OVER w_holder_id_product, 2) AS premium_vs_holder_id_product_avg,
        ROUND(CUME_DIST() OVER (ORDER BY end_date ASC), 3) AS end_date_cume_dist,
        LEAD(end_date) OVER w_product_holder_id_by_end_dat AS next_end_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, product), 0), 4) AS premium_share_of_holder_id_product,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY product, YEAR(end_date) ORDER BY premium DESC NULLS FIRST, id)) AS days_since_prev_end_date,
        COUNT(*) OVER (PARTITION BY product, holder_id) AS rows_in_product_holder_id,
        LEAD(end_date, 2) OVER w_product_end_date_year_by_pre AS next2_end_date,
        MAX(premium) OVER (PARTITION BY product, holder_id ORDER BY end_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_max_6_prior
    FROM cleaned
    WINDOW w_product_by_premium AS (PARTITION BY product ORDER BY premium, id), w_holder_id_product AS (PARTITION BY holder_id, product), w_product_holder_id_by_end_dat AS (PARTITION BY product, holder_id ORDER BY end_date DESC, id), w_product_end_date_year_by_pre AS (PARTITION BY product, YEAR(end_date) ORDER BY premium DESC NULLS FIRST, id)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_all_by_premium_max_6_prior AS premium_max_6_prior_rank,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY premium_max_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        ROUND(premium_max_6_prior / NULLIF(SUM(premium_max_6_prior) OVER (PARTITION BY YEAR(next2_end_date)), 0), 4) AS premium_max_6_prior_share_of_next2_end_date_year
    FROM sequenced
    WINDOW w_all_by_premium_max_6_prior AS (ORDER BY premium_max_6_prior, id)
)
SELECT
    id,
    premium,
    end_date,
    product_clean,
    premium_percent_rank,
    premium_vs_holder_id_product_avg,
    end_date_cume_dist,
    next_end_date,
    premium_share_of_holder_id_product,
    days_since_prev_end_date,
    next2_end_date,
    first_known_end_date,
    premium_max_6_prior_share_of_next2_end_date_year
FROM metrics
WHERE premium_vs_holder_id_product_avg >= -1500.0
ORDER BY first_known_end_date DESC NULLS FIRST, premium DESC NULLS FIRST
