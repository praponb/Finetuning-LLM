from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.month("delivered_date").isin([2, 8, 12]) & (F.col("order_id") == 3))
    .withColumn("delivered_date_month", F.month("delivered_date"))
)

shipments_stats = (
    base
    .groupBy(
        "carrier",
        F.when(F.col("delivered_date_month") >= 65, "high").when(F.col("delivered_date_month") >= 22, "mid").otherwise("low").alias("delivered_date_month_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
    )
)

result = shipments_stats.select("carrier", "pct_flash")
