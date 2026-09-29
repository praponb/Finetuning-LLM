from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("amount_tier", F.when(F.col("amount") >= 3100, "PREMIUM").otherwise("STANDARD"))
    .cube("amount_tier")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.sum("amount").alias("total_amount"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("total_amount") < 0)
    .orderBy("total_amount")
)
