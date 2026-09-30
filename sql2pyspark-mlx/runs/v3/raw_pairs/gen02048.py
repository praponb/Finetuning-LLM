from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Singapore", "London", "Bangkok"]))
    .groupBy("country")
    .agg(
        F.avg("age").alias("avg_age"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count("age").alias("age_count"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
    )
    .filter(F.col("us_count") < 5)
    .drop("us_count")
)
