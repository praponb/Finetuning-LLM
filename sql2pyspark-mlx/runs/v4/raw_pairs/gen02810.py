from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
    )
    .filter(F.col("pct_flash") > 65)
    .orderBy("carrier")
    .limit(20)
)
