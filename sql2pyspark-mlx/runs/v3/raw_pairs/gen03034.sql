select id, order_id, status, case when status in ('FAILED', 'SUCCESS') then 'Tier 1' else 'Other' end as status_group, length(status) as status_length from payments order by status_length desc;
