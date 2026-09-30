from pyspark.sql import functions as F

src = df_orders.filter(F.col("store_id").isin([17, 37]) | F.col("store_id").isNull())

status_group_summary = (
    src
    .groupBy(
        F.when(F.col("status").isin(["PENDING", "COMPLETED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_completed"),
    )
)

result = status_group_summary.select("status_group", "status_list", "pct_completed")
