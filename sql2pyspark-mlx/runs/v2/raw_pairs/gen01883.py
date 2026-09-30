from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") < 2000))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

summary = (
    base
    .groupBy("merchant")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    summary
    .filter(F.col("total_amount") < 9000)
    .select("merchant", "total_amount")
    .orderBy("total_amount")
)
