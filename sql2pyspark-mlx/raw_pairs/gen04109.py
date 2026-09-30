from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") > 62) | ~F.col("segment").isin(["Corporate", "SMB"]))
    .select("city", F.when(F.col("country") == "US", 1).otherwise(0).alias("is_us"))
    .orderBy("city", "is_us")
)
