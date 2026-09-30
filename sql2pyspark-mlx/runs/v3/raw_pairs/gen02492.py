from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([35, 37]) & (F.col("order_date") < "2023-07-13"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "web", 1).otherwise(0))) / F.count("*")), 1).alias("pct_web"),
    )
    .orderBy(F.col("pct_web").desc())
)
