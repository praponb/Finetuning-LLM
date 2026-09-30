WITH flattened AS (
    SELECT
        id,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS element
),
cleaned AS (
    SELECT *, ADD_MONTHS(effective_from, 12) AS effective_from_plus_months
    FROM flattened
    WHERE effective_from >= DATE '2024-09-29'
),
enriched AS (
    SELECT
        *,
        RANK() OVER w_running AS element_rank,
        RANK() OVER (ORDER BY element NULLS FIRST) AS element_rank_2
    FROM cleaned
    WINDOW w_running AS (ORDER BY element DESC)
),
with_windows AS (
    SELECT
        id,
        effective_from,
        element,
        effective_from_plus_months,
        element_rank,
        element_rank_2,
        CUME_DIST() OVER (ORDER BY element_rank_2 NULLS LAST) AS element_rank_2_cume_dist,
        RANK() OVER (ORDER BY element DESC) AS element_rank_3,
        CUME_DIST() OVER w_ordered AS element_rank_cume_dist
    FROM enriched
    WINDOW w_ordered AS (ORDER BY element_rank DESC)
)
SELECT
    id,
    effective_from,
    element,
    effective_from_plus_months,
    element_rank,
    element_rank_2_cume_dist
FROM with_windows;
