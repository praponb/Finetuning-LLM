select account_type_group, balance_range
from (
    select case when account_type in ('SAVINGS', 'CHECKING') then 'tier_1' else 'other' end as account_type_group, max(balance) - min(balance) as balance_range
    from accounts
    group by case when account_type in ('SAVINGS', 'CHECKING') then 'tier_1' else 'other' end
) as s
where balance_range >= 61
order by account_type_group desc, balance_range desc
