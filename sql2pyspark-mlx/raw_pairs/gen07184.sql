with filtered as (
    select
        *,
        datediff(current_date(), ends_on) as days_since_ends_on,
        coalesce(channel, 'OTHER') as channel_clean
    from promotions
    where ends_on < '2023-03-01'
)
select promo_code, id
from filtered
except all
select promo_code, id
from promotions
where starts_on < '2024-06-21';
