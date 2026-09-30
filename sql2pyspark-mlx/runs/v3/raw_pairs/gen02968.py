from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        F.when(F.col("channel").isin(["store", "partner"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .filter(F.col("pct_web") < 60)
    .orderBy(F.col("status").desc())
)
