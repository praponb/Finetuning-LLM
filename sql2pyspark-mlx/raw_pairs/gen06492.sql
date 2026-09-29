with attributed as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct as promotion_discount_pct
    from promotions s
    left join promotions pr
        on s.channel = pr.channel
       and s.starts_on >= pr.starts_on
       and s.starts_on < pr.ends_on + interval 7 days
),
latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by promo_code order by ends_on desc, id, promo_id) as row_num
    from attributed
    qualify row_num = 1
),
cleaned as (
    -- Keep recent, valid rows
    select
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_id,
        row_num,
        upper(channel) as channel_upper,
        coalesce(promotion_promo_code, 'unassigned') as promotion_promo_code_filled
    from latest
    where ends_on >= date '2023-06-24'
      and promotion_promo_code is not null
),
attributed_promotions as (
    -- Non-equi join on a validity window
    select
        s.*,
        pr.id as promo_id_2,
        pr.promo_code as promotion_promo_code,
        pr.discount_pct
    from cleaned s
    left join promotions pr
        on s.channel = pr.channel
       and s.ends_on >= pr.starts_on
       and s.ends_on <= date_add(pr.ends_on, 7)
),
matched_keys as (
    -- Set difference on keys
    select promo_code
    from attributed_promotions
    except
    select promo_code
    from promotions
    where channel in ('web', 'mobile', 'store')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from attributed_promotions s
    inner join matched_keys k on s.promo_code = k.promo_code
),
scored as (
    -- Ranking and gap features
    select
        id,
        promo_code,
        starts_on,
        ends_on,
        channel_upper,
        promotion_promo_code_filled,
        promo_id_2,
        percent_rank() over (order by promo_id_2 desc nulls first) as promo_id_2_percent_rank,
        rank() over (order by promo_id_2 desc) as promo_id_2_rank
    from reconciled
)
select
    id,
    starts_on,
    ends_on,
    promotion_promo_code_filled,
    promo_id_2,
    promo_id_2_rank
from scored
order by promo_id_2_rank desc
