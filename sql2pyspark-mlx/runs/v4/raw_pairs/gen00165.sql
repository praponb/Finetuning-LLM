select *, if(is_flagged, 'Yes', 'No') as is_flagged_label, floor(amount) as amount_whole
from transactions
