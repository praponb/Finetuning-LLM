WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL '14' DAY
),
matched_promotions AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM matched a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on <= DATE_ADD(pr.ends_on, 10)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY promo_id_2, id, promo_id, promo_id_2) AS seq_no,
        AVG(promotion_discount_pct_2) OVER (PARTITION BY promotion_promo_code ORDER BY promo_id_2, id, promo_id, promo_id_2 ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS promotion_discount_pct_2_avg_2_prior
    FROM matched_promotions
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        channel,
        starts_on,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        seq_no,
        promotion_discount_pct_2_avg_2_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct, id, promo_id, promo_id_2) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_3,
        pr.promo_code,
        pr.discount_pct
    FROM latest e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_id,
        promotion_promo_code,
        promotion_discount_pct,
        promo_id_2,
        promotion_promo_code_2,
        seq_no,
        promo_id_3,
        promo_code,
        LEAD(promo_id_3, 1) OVER (PARTITION BY promotion_promo_code_2 ORDER BY promo_id, id, promo_id, promo_id_2, promo_id_3) AS next_promo_id_3,
        PERCENT_RANK() OVER (PARTITION BY promo_code ORDER BY promo_id_3 NULLS FIRST) AS promo_id_3_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct, id, promo_id, promo_id_2, promo_id_3) AS row_seq
    FROM attributed
    WHERE promotion_promo_code_2 IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics_promotions
    EXCEPT
    SELECT id
    FROM promotions
    WHERE channel IN ('store', 'partner')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_promotions s
    INNER JOIN remaining_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    seq_no,
    promo_code,
    CASE WHEN promotion_discount_pct >= 0.32 THEN 'HIGH' ELSE 'LOW' END AS promotion_discount_pct_tier,
    SUM(CASE WHEN promotion_promo_code_2 = 'WELCOME5' THEN promo_id_3_percent_rank ELSE 0 END) AS welcome5_promo_id_3_percent_rank,
    SORT_ARRAY(COLLECT_SET(promotion_promo_code)) AS promotion_promo_code_list,
    COUNT(*) AS event_count,
    COUNT(*) AS event_count_2,
    COUNT(*) FILTER (WHERE promotion_promo_code = 'SPRING10') AS spring10_rows
FROM kept_rows
GROUP BY seq_no, promo_code, CASE WHEN promotion_discount_pct >= 0.32 THEN 'HIGH' ELSE 'LOW' END
ORDER BY spring10_rows DESC;
