with flattened as (
    select
        id,
        currency,
        element
    from price_tiers
    lateral view outer explode(discounts) e as element
),
matched_keys as (
    select currency
    from flattened
    except all
    select currency
    from price_tiers
    where currency = 'USD'
),
reconciled as (
    select s.*
    from flattened s
    join matched_keys k on s.currency = k.currency
),
enriched as (
    select
        *,
        percent_rank() over (partition by currency order by element asc) as element_percent_rank,
        rank() over (partition by currency order by element desc nulls last) as element_rank,
        sum(element) over (partition by currency) as currency_sum_element,
        sum(element) over (partition by currency) as currency_sum_element_2
    from reconciled
),
scored as (
    select
        id,
        currency,
        element,
        element_percent_rank,
        currency_sum_element,
        cume_dist() over (partition by currency order by currency_sum_element_2 desc nulls last) as currency_sum_element_2_cume_dist,
        sum(currency_sum_element) over w_hist as currency_sum_currency_sum_element,
        round(element_rank / sum(element_rank) over (partition by currency), 4) as element_rank_share,
        percent_rank() over w_running as element_percent_rank_percent_rank,
        stddev_samp(currency_sum_element) over w_hist as currency_stddev_samp_currency_sum_element
    from enriched
    window w_hist as (partition by currency), w_running as (partition by currency order by element_percent_rank desc nulls first)
)
select
    id,
    element,
    element_percent_rank,
    currency_sum_element,
    currency_sum_element_2_cume_dist,
    currency_sum_currency_sum_element,
    element_percent_rank_percent_rank
from scored
order by element_percent_rank desc nulls first, currency_sum_element desc;
