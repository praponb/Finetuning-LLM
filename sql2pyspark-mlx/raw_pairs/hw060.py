from pyspark.sql import functions as F

result = df_customers.select("id", F.when(F.col("country").isin(["US", "UK", "DE"]), "West").otherwise("Asia").alias("market"))
