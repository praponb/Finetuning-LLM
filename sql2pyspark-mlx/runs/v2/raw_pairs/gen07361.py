from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.round(((100 * F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grab"),
    )
    .orderBy("pct_grab")
)
