from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", F.lit("store").alias("source"))
    .union(
        df_orders
        .filter(F.col("status").isin(["RETURNED", "COMPLETED"]))
        .select("order_date", F.lit("order").alias("source"))
    )
)
