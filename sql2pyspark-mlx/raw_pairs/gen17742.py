from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("activity_month").orderBy(F.col("completed_count").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.sqft",
        F.col("c.revenue"),
        F.col("c.units").alias("store_sale_units"),
    )
)

latest = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_name", "sqft")
)

with_latest_stores = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", "s.sqft", F.col("c.status").alias("order_status"))
)

with_last = (
    with_latest_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", "s.order_status", F.col("c.last_restock"))
)

kept = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 6, 8])), "left_semi")
)

result = (
    kept
    .groupBy(
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
        "order_status",
    )
    .agg(
        F.count_if(F.col("order_status") == "SHIPPED").alias("shipped_events"),
        F.count(F.when(F.col("order_status") == "COMPLETED", 1)).alias("completed_count"),
        F.count(F.when(F.col("order_status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .withColumn("completed_count_rank", F.rank().over(w5))
)
