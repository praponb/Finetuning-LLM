from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("created_at", "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("attributes")["device"].alias("device"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    flattened_attrs
    .withColumn("last_known_utm_source", F.last("utm_source", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
)
