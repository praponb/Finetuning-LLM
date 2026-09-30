from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(~F.col("city").isin(["New York", "Bangkok"])).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.amount"))
    .orderBy(F.col("id").desc())
)
