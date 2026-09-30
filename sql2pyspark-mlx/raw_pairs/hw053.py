from pyspark.sql import functions as F

result = df_customers.select("id", F.coalesce("city", "country", F.lit("unknown")).alias("location"))
