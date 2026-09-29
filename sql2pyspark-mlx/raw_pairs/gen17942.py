from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("is_active") & (F.col("city") == "Chiang Mai")) | F.col("country").isin(["TH", "US"]))
    .withColumn("country_name_key", F.concat_ws(",", "country", "name"))
)

customers_stats = (
    filtered_customers
    .groupBy("city")
    .agg(F.max("age").alias("max_age"))
)

result = customers_stats.filter(F.col("max_age") > 47)
