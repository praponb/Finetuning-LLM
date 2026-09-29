WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE status IN ('ACTIVE', 'CANCELLED', 'LAPSED')
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_holder_region_holder_id_by_e AS end_date_dense_rank,
        ROW_NUMBER() OVER w_holder_region_holder_id_by_e_2 AS rn,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        LAG(end_date) OVER (PARTITION BY holder_region, holder_id ORDER BY end_date DESC, id) AS prev_end_date,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY end_date DESC) AS end_date_rank,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY end_date DESC, id) AS end_date_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY end_date, id), 3) AS end_date_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), product ORDER BY end_date DESC), 3) AS end_date_percent_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_region, holder_id ORDER BY end_date DESC), 3) AS end_date_percent_rank_3,
        NTILE(5) OVER w_all_by_end_date AS end_date_quintile,
        LEAD(end_date, 2) OVER (PARTITION BY holder_region, holder_id ORDER BY end_date DESC, id) AS next2_end_date,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_region, holder_id ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date
    FROM with_details
    WINDOW w_holder_region_holder_id_by_e AS (PARTITION BY holder_region, holder_id ORDER BY end_date DESC), w_holder_region_holder_id_by_e_2 AS (PARTITION BY holder_region, holder_id ORDER BY end_date DESC, id), w_all_by_end_date AS (ORDER BY end_date, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY end_date_rank ORDER BY end_date_dense_rank_2 ASC NULLS LAST), 3) AS end_date_dense_rank_2_cume_dist,
        LAST_VALUE(end_date_rank) IGNORE NULLS OVER (PARTITION BY end_date_rank ORDER BY end_date_dense_rank_2 ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date_rank,
        SUM(rn) OVER (PARTITION BY rn, YEAR(prev_end_date)) AS rn_prev_end_date_year_sum_rn,
        LAST_VALUE(prev_end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_end_date), MONTH(next2_end_date) ORDER BY end_date_dense_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_end_date
    FROM sequenced
)
SELECT
    id,
    holder_id,
    product,
    status,
    holder_segment,
    holder_region,
    first_known_end_date,
    end_date_dense_rank_2,
    end_date_percent_rank_3,
    end_date_quintile,
    last_known_end_date,
    last_prev_end_date,
    INITCAP(TRIM(product)) AS product_clean,
    LENGTH(TRIM(holder_segment)) AS holder_segment_len
FROM ranked
ORDER BY holder_segment, last_known_end_date DESC
