from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("region") == "South")
    .withColumn("months_since_opened_date", (F.datediff(F.current_date(), "opened_date") / 30))
    .withColumn("region_clean", F.coalesce("region", F.lit("N/A")))
)

result = (
    valid
    .groupBy("city")
    .pivot("region", ["Central", "East", "South"])
    .agg(F.sum("sqft").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "city",
        F.col("Central_amount").alias("central_amount"),
        F.col("Central_item_count").alias("central_item_count"),
        F.col("East_amount").alias("east_amount"),
        F.col("East_item_count").alias("east_item_count"),
        F.col("South_amount").alias("south_amount"),
        F.col("South_item_count").alias("south_item_count"),
    )
)
