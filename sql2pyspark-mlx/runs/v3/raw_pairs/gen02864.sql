select id, manager_id, job_title, is_remote, case when is_remote then 'Yes' else 'No' end is_remote_label from employees where job_title <> 'Engineer'
