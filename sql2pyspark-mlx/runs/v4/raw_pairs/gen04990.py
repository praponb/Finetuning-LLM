from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-24").cast("date")) & (F.col("opened_date") >= "2023-07-01"))
    .select(
        "*",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    base
    .groupBy("region")
    .pivot("city", ["New York", "Chiang Mai", "London"])
    .agg(F.avg("sqft"))
    .select(
        "region",
        F.col("New York").alias("new_york"),
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("London").alias("london"),
    )
)
