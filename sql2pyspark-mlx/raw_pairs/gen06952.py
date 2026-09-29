from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("city", ["London", "New York"])
    .agg(F.max("age"))
    .select(
        "segment",
        F.col("London").alias("london"),
        F.col("New York").alias("new_york"),
    )
)
