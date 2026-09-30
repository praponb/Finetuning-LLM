WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(discount_pct) OVER w_grp AS prev_discount_pct,
        LEAD(discount_pct) OVER w_grp AS next_discount_pct,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM promotions
    WINDOW w_grp AS (PARTITION BY channel ORDER BY ends_on, id)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM metrics src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on <= DATE_ADD(pr.ends_on, 3)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(promo_id) OVER w_recent AS running_sum_promo_id,
        COUNT(promo_id) OVER w_recent AS running_count_promo_id,
        MAX(promo_id) OVER w_recent AS running_max_promo_id
    FROM attributed
    WHERE row_seq < 7
    WINDOW w_recent AS (PARTITION BY channel, promotion_promo_code ORDER BY row_seq, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_windows e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on <= DATE_ADD(pr.ends_on, 10)
),
matched_keys AS (
    -- Set difference on keys
    SELECT promo_code, id
    FROM with_window
    EXCEPT DISTINCT
    SELECT promo_code, id
    FROM promotions
    WHERE channel IN ('store', 'mobile')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_window s
    INNER JOIN matched_keys k ON s.promo_code = k.promo_code AND s.id = k.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        promotion_promo_code,
        promotion_promo_code_2,
        running_sum_promo_id
    FROM reconciled
) src
PIVOT (
    AVG(running_sum_promo_id)
    FOR promotion_promo_code_2 IN ('BUNDLE', 'VIP', 'WELCOME5')
);
