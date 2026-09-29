from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") != "PENDING") & (F.col("discount") <= 0.1) & F.col("discount").between(0.28, 0.36))
    .groupBy("channel")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
        F.countDistinct("discount").alias("unique_stores"),
    )
    .filter(F.col("unique_stores") >= 5)
    .orderBy(F.col("channel").desc())
)
