from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("order_id") == 5)
    .withColumn("shipped_date_plus_months", F.add_months("shipped_date", 6))
)

summary = (
    valid_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
    )
)

result = (
    summary
    .filter(F.col("pct_flash") >= 50)
    .select("delivered_quarter", "avg_cost", "pct_flash")
    .orderBy(F.col("avg_cost").desc())
)
