from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_tier", F.when(F.col("cost") >= 77, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .groupingSets(
        [["cost_tier", "quarter_start"], ["cost_tier"], []],
        "cost_tier",
        "quarter_start",
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("cost_per_lost"),
        F.sum(F.when(F.col("status") == "DELIVERED", F.col("cost")).otherwise(0)).alias("delivered_cost"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("cost_per_lost_2"),
    )
)
