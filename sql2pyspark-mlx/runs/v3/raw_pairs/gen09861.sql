select amount, substring(method, 1, 2) method_prefix, concat(method, ', ', status) method_status
from payments;
