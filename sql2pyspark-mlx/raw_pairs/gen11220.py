from pyspark.sql import functions as F

result = df_suppliers.select("id", "city", "supplier_name")
