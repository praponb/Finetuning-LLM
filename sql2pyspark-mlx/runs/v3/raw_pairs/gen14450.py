from pyspark.sql import functions as F

result = df_suppliers.filter(F.lower(F.trim("supplier_name")) == "tokyo traders")
