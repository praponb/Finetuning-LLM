from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"))
)

result = (
    latest_match
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.sum(F.when(F.col("region") == "South", F.col("amount")).otherwise(0)).alias("south_amount"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy("region")
)
