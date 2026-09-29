select status, sum(case when account_type = 'LOAN' then 1 else 0 end) loan_count from accounts group by status having avg(balance) > 61000 order by status desc, loan_count;
