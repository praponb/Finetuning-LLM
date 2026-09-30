from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("region")
w4 = Window.partitionBy("region", "city").orderBy("order_order_date", "id")

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") >= F.col("s.opened_date")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"), F.col("c.sale_date"))
)

latest_match_stores = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"))
)

enriched = (
    latest_match_stores
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4))
    .withColumn("seq_no", F.row_number().over(w4))
)

result = (
    enriched
    .groupBy("region")
    .pivot("city", ["New York", "Bangkok", "London"])
    .agg(F.avg("seq_no").alias("total"))
    .select(
        "region",
        F.col("New York").alias("new_york"),
        F.col("Bangkok").alias("bangkok"),
        F.col("London").alias("london"),
    )
)
