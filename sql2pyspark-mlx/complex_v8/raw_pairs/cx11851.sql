WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE end_date < DATE '2026-04-25'
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM scoped s
    JOIN policyholders p ON p.id = s.holder_id AND LENGTH(TRIM(p.holder_name)) >= 7
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(holder_risk_score) OVER (PARTITION BY status ORDER BY start_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_holder_risk_score,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_region, DATE_TRUNC('MONTH', start_date) ORDER BY holder_risk_score DESC), 3) AS holder_risk_score_percent_rank,
        SUM(holder_risk_score) OVER w_start_date_month AS start_date_month_sum_holder_risk_score,
        COUNT(*) OVER w_start_date_month AS rows_in_start_date_month,
        LEAD(premium, 1, 0) OVER (PARTITION BY holder_region, status ORDER BY start_date DESC, id) AS next_premium,
        NTILE(5) OVER (PARTITION BY MONTH(start_date), holder_id ORDER BY start_date DESC NULLS FIRST, id) AS start_date_quintile,
        holder_risk_score - LAG(holder_risk_score) OVER w_holder_region_status_by_prem AS holder_risk_score_change,
        COUNT(*) OVER w_status AS rows_in_status,
        NTH_VALUE(start_date, 2) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM enriched
    WINDOW w_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', start_date)), w_holder_region_status_by_prem AS (PARTITION BY holder_region, status ORDER BY premium DESC, id), w_status AS (PARTITION BY status)
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        id,
        premium,
        start_date,
        holder_risk_score,
        holder_region,
        running_holder_risk_score,
        start_date_month_sum_holder_risk_score,
        rows_in_start_date_month,
        start_date_quintile,
        holder_risk_score_change,
        INITCAP(TRIM(holder_region)) AS holder_region_clean,
        premium - start_date_quintile AS premium_minus_start_date_quintile
    FROM ranked
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(running_holder_risk_score) OVER (PARTITION BY start_date_quintile, holder_region) AS start_date_quintile_holder_region_min_running_ho,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY start_date_quintile, holder_region), 0), 4) AS holder_risk_score_share_of_start_date_quintile_h,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date_quintile DESC NULLS FIRST, id), 3) AS start_date_quintile_percent_rank,
        LEAD(start_date_quintile, 2) OVER w_all_by_start_date_quintile AS next2_start_date_quintile,
        COUNT(*) OVER (ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(start_date_quintile) OVER (PARTITION BY holder_region, start_date_quintile ORDER BY start_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS start_date_quintile_sum_6_prior,
        RANK() OVER w_all_by_holder_risk_score_tie AS holder_risk_score_rank
    FROM enriched_2
    WINDOW w_all_by_start_date_quintile AS (ORDER BY start_date_quintile DESC NULLS FIRST, id), w_all_by_holder_risk_score_tie AS (ORDER BY holder_risk_score DESC)
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    holder_region,
    running_holder_risk_score,
    start_date_month_sum_holder_risk_score,
    rows_in_start_date_month,
    start_date_quintile,
    holder_risk_score_change,
    holder_region_clean,
    premium_minus_start_date_quintile,
    start_date_quintile_holder_region_min_running_ho,
    start_date_quintile_percent_rank,
    next2_start_date_quintile,
    running_count,
    start_date_quintile_sum_6_prior,
    holder_risk_score_rank,
    holder_risk_score_rank + holder_risk_score_change AS holder_risk_score_rank_plus_holder_risk_score_ch
FROM ranked_2
ORDER BY holder_risk_score_rank NULLS LAST
