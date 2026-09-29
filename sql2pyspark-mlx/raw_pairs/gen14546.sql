WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        ROUND(DATEDIFF(starts_on, ends_on) / 30, 1) AS months_ends_on_to_starts_on,
        INITCAP(TRIM(promo_code)) AS promo_code_title
    FROM promotions
    WHERE channel IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        months_ends_on_to_starts_on,
        promo_code_title,
        LAST_DAY(starts_on) AS starts_on_month_end,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM filtered
    WHERE channel IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        months_ends_on_to_starts_on,
        starts_on_month_end,
        MIN(months_ends_on_to_starts_on) OVER w_grp AS running_min_months_ends_on_to_starts_on,
        SUM(months_ends_on_to_starts_on) OVER w_grp AS running_sum_months_ends_on_to_starts_on,
        MAX(months_ends_on_to_starts_on) OVER w_grp AS running_max_months_ends_on_to_starts_on,
        COUNT(months_ends_on_to_starts_on) OVER w_grp AS running_count_months_ends_on_to_starts_on
    FROM deduped
    WINDOW w_grp AS (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        months_ends_on_to_starts_on,
        starts_on_month_end,
        running_sum_months_ends_on_to_starts_on,
        running_max_months_ends_on_to_starts_on,
        running_count_months_ends_on_to_starts_on,
        DATEDIFF(ends_on, starts_on_month_end) AS days_starts_on_month_end_to_ends_on
    FROM metrics
    WHERE running_sum_months_ends_on_to_starts_on IS NOT NULL
)
SELECT
    id,
    starts_on,
    ends_on,
    discount_pct,
    months_ends_on_to_starts_on,
    running_sum_months_ends_on_to_starts_on,
    running_max_months_ends_on_to_starts_on,
    running_count_months_ends_on_to_starts_on,
    days_starts_on_month_end_to_ends_on
FROM valid;
