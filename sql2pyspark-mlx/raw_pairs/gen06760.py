from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("customer_id").isin([7, 14, 31]) & (F.col("status") == "PENDING") & F.col("channel").isin(["partner", "mobile", "store"]))
)

agg = (
    base
    .groupBy("channel")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
    )
)

result = (
    agg
    .filter(F.col("pct_partner") >= 26)
    .orderBy(F.col("pct_partner").desc(), "channel")
    .limit(3)
)
