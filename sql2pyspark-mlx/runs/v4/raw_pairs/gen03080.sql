WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS element
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS FIRST) AS element_rank,
        COUNT(element) OVER w_part AS currency_count_element,
        DENSE_RANK() OVER w_ordered AS element_dense_rank
    FROM lines
    WINDOW w_part AS (PARTITION BY currency), w_ordered AS (PARTITION BY currency ORDER BY element ASC NULLS FIRST)
),
matched_keys AS (
    SELECT currency, id
    FROM enriched
    EXCEPT ALL
    SELECT currency, id
    FROM price_tiers
    WHERE currency NOT IN ('THB', 'EUR', 'USD')
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.currency = k.currency AND s.id = k.id
),
scored AS (
    SELECT
        *,
        ROUND(currency_count_element / SUM(currency_count_element) OVER (PARTITION BY currency), 4) AS currency_count_element_share,
        ROUND(element_dense_rank / SUM(element_dense_rank) OVER (PARTITION BY plan), 4) AS element_dense_rank_share,
        CUME_DIST() OVER (ORDER BY element ASC) AS element_cume_dist
    FROM kept_rows
)
SELECT
    id,
    currency,
    element,
    currency_count_element,
    element_dense_rank,
    currency_count_element_share,
    element_dense_rank_share,
    element_cume_dist,
    CASE WHEN currency RLIKE '^(THB|USD)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS currency_zone
FROM scored
ORDER BY id DESC NULLS LAST;
