select txn_type from transactions where merchant <> 'Grab' union select txn_type from transactions where merchant in ('Amazon', '7-Eleven') order by txn_type;
