WITH latest AS (
    SELECT id, discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS latest_rank
        FROM promotions
    ) t
    WHERE latest_rank = 1
),
enriched AS (
    SELECT
        *,
        COUNT(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_discount_pct,
        SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_discount_pct,
        MAX(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_discount_pct
    FROM latest
    WHERE discount_pct < 0.31
),
base AS (
    SELECT *, CAST(last7_count_discount_pct AS DOUBLE) AS last7_count_discount_pct_num
    FROM enriched
    WHERE last7_sum_discount_pct < 1.58
),
matched_keys AS (
    SELECT id
    FROM base
    EXCEPT ALL
    SELECT id
    FROM promotions
    WHERE discount_pct >= 0.12
),
reconciled AS (
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    SELECT *, last7_count_discount_pct_num - last7_max_discount_pct AS last7_count_discount_pct_num_minus_last7_max_discount_pct
    FROM reconciled
    WHERE last7_count_discount_pct BETWEEN 8 AND 10
)
SELECT
    id,
    discount_pct,
    last7_count_discount_pct,
    last7_count_discount_pct_num,
    last7_count_discount_pct_num_minus_last7_max_discount_pct,
    PERCENT_RANK() OVER (ORDER BY last7_sum_discount_pct) AS last7_sum_discount_pct_percent_rank,
    PERCENT_RANK() OVER (ORDER BY last7_count_discount_pct_num_minus_last7_max_discount_pct DESC NULLS LAST) AS last7_count_discount_pct_num_minus_last7_max_discount_pct_percent_rank,
    CUME_DIST() OVER (ORDER BY last7_count_discount_pct_num DESC NULLS LAST) AS last7_count_discount_pct_num_cume_dist,
    CUME_DIST() OVER (ORDER BY last7_count_discount_pct_num DESC NULLS LAST) AS last7_count_discount_pct_num_cume_dist_2,
    CUME_DIST() OVER (ORDER BY discount_pct) AS discount_pct_cume_dist
FROM valid;
