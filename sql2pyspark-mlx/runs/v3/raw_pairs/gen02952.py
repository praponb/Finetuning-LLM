from pyspark.sql import functions as F

result = df_suppliers.select("country", "supplier_name")
