from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
    .orderBy(F.col("pct_returned").desc())
)
