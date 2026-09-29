from pyspark.sql import functions as F

result = df_suppliers.select("rating", "country", "supplier_name")
