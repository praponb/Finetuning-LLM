WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date
    FROM policies
    WHERE status <> 'EXPIRED'
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        COUNT(*) AS cnt
    FROM filtered_policys
    GROUP BY 1
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(end_date_ym)) AS end_date_ym_len,
        LOWER(TRIM(end_date_ym)) AS end_date_ym_clean,
        FLOOR(cnt / 2) * 2 AS cnt_bucket
    FROM per_group
),
exploded AS (
    -- One row per product_list_item
    SELECT
        s.end_date_ym,
        s.cnt,
        s.end_date_ym_len,
        s.end_date_ym_clean,
        s.cnt_bucket,
        t.product_list_item
    FROM scored s
    LATERAL VIEW EXPLODE(s.product_list) t AS product_list_item
),
tagged AS (
    -- Add calculated columns
    SELECT
        end_date_ym,
        cnt,
        end_date_ym_len,
        cnt_bucket,
        product_list_item,
        COALESCE(cnt, 0) AS cnt_filled,
        SUBSTRING(TRIM(product_list_item), 1, 3) AS product_list_item_prefix
    FROM exploded
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY cnt_filled, cnt ORDER BY cnt ASC NULLS LAST), 3) AS cnt_cume_dist,
        LAST_VALUE(cnt_bucket) IGNORE NULLS OVER (PARTITION BY cnt_filled, cnt ORDER BY cnt ASC NULLS LAST, end_date_ym, product_list_item ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cnt_bucket,
        ROUND(PERCENT_RANK() OVER (PARTITION BY cnt_filled, cnt ORDER BY cnt ASC NULLS LAST), 3) AS cnt_percent_rank,
        MAX(cnt) OVER w_cnt AS cnt_max_cnt
    FROM tagged
    WINDOW w_cnt AS (PARTITION BY cnt)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (ORDER BY cnt_max_cnt DESC) AS cnt_max_cnt_rank,
        ROUND(cnt_cume_dist - AVG(cnt_cume_dist) OVER w_cnt_bucket, 2) AS cnt_cume_dist_vs_cnt_bucket_avg,
        ROUND(CUME_DIST() OVER (ORDER BY cnt_max_cnt DESC), 3) AS cnt_max_cnt_cume_dist
    FROM with_trends
    WINDOW w_cnt_bucket AS (PARTITION BY cnt_bucket)
)
SELECT
    end_date_ym,
    cnt,
    end_date_ym_len,
    cnt_bucket,
    product_list_item_prefix,
    cnt_cume_dist,
    last_known_cnt_bucket,
    cnt_percent_rank,
    cnt_max_cnt,
    cnt_cume_dist_vs_cnt_bucket_avg,
    cnt_max_cnt_cume_dist,
    CONCAT(product_list_item_prefix, '#', CAST(cnt_bucket AS STRING)) AS product_list_item_prefix_ref
FROM windowed
ORDER BY cnt_bucket
