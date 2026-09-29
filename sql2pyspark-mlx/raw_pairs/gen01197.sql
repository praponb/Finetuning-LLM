select d.status, a.merchant, count(*) as cnt, min(a.amount) as min_amount, grouping_id() as gid from transactions a left join accounts d on a.account_id = d.id group by rollup (d.status, a.merchant);
