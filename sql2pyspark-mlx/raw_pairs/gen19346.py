from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

summary = (
    base
    .groupBy("is_flagged")
    .agg(
        (F.max("is_flagged_flag") - F.min("is_flagged_flag")).alias("is_flagged_flag_range"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = summary
