from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("amount") <= 670).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.sqft") == 14958)
    .select(F.col("s.id"), F.col("s.region"))
    .orderBy("id", "region")
)
