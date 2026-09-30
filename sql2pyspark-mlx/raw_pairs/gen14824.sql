WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft
    FROM (
        SELECT *, RANK() OVER (PARTITION BY region, city ORDER BY opened_date DESC NULLS LAST) AS latest_rank
        FROM stores
    ) ranked
    WHERE latest_rank = 1
),
with_last AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.sale_date
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY MONTH(opened_date), city), 4) AS sqft_share,
        SUM(sqft) OVER (PARTITION BY YEAR(opened_date), city) AS opened_date_year_sum_sqft,
        LAST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sqft,
        SUM(sqft) OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date), region ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM with_last
),
enriched_stores AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(opened_date_year_sum_sqft) OVER w_ordered AS next_opened_date_year_sum_sqft,
        LAG(sqft_share) OVER w_ordered AS prev_sqft_share,
        LEAD(sqft_share) OVER w_ordered AS next_sqft_share,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY region ORDER BY sqft_share, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date >= s.opened_date
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (10, 32, 34)
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        sqft_share,
        opened_date_year_sum_sqft,
        next_opened_date_year_sum_sqft,
        next_sqft_share,
        STDDEV_SAMP(seq_no) OVER w_recent AS city_stddev_samp_seq_no,
        SUM(last_known_sqft) OVER (PARTITION BY region, city ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_known_sqft,
        LEAD(last_known_sqft, 2) OVER (PARTITION BY city ORDER BY cumulative_sqft, id) AS next_last_known_sqft
    FROM kept
    WINDOW w_recent AS (PARTITION BY city, region)
),
daily AS (
    -- Multi-level aggregation
    SELECT
        city_stddev_samp_seq_no,
        MIN(sqft_share) AS min_sqft_share,
        APPROX_PERCENTILE(next_opened_date_year_sum_sqft, 0.9) AS next_opened_date_year_sum_sqft_p90
    FROM with_windows
    GROUP BY city_stddev_samp_seq_no
    HAVING MIN(sqft_share) > 0
)
SELECT
    city_stddev_samp_seq_no,
    min_sqft_share,
    next_opened_date_year_sum_sqft_p90
FROM daily;
