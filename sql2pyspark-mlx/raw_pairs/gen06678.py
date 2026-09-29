from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy(F.col("units").desc_nulls_last(), "id")
w_recent = Window.partitionBy("city").orderBy(F.col("order_discount").desc(), "id")
w_ordered = Window.partitionBy("city").orderBy(F.col("revenue").desc(), "id")

with_windows = (
    df_stores
    .filter(F.col("city") == "London")
    .withColumn("running_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_avg_6", F.avg("sqft").over(w2))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w3))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.opened_date",
        "s.running_sqft",
        "s.cumulative_sqft",
        F.col("c.discount").alias("order_discount"),
    )
)

with_latest_stores = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.running_sqft",
        "s.cumulative_sqft",
        "s.order_discount",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units"),
    )
)

eligible = (
    with_latest_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.store_sale_sale_date")), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

deduped = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "city", "order_discount", "revenue")
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "order_discount",
        F.ntile(5).over(w_recent).alias("order_discount_bucket"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)
