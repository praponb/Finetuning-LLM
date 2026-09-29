from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("pct_partner").desc())
)
