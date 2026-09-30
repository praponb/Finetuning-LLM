from pyspark.sql import functions as F

result = df_suppliers.filter(F.col("country").isin(["JP", "DE"]))
