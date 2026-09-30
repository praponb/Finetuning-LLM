from pyspark.sql import functions as F

result = df_suppliers.filter((F.col("city") != "Berlin") | (F.col("city") != "Tokyo"))
