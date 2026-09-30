from pyspark.sql import functions as F

result = df_suppliers.select("id", "rating", "supplier_name", "country")
