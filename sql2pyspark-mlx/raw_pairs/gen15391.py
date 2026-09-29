from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .pivot("city", ["New York", "Bangkok", "Singapore"])
    .agg(F.avg("rating").alias("value_sum"))
    .select(
        "country",
        F.col("New York").alias("new_york"),
        F.col("Bangkok").alias("bangkok"),
        F.col("Singapore").alias("singapore"),
    )
)
