from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("basket_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("basket_amount").desc(), "id")
w3 = Window.partitionBy(F.month("created_at"), "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy(F.col("basket_amount").asc_nulls_last())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    flattened_attrs
    .withColumn("last_known_device_or_default", F.last("device_or_default", ignorenulls=True).over(w))
    .withColumn("basket_amount_bucket", F.ntile(5).over(w2))
    .withColumn("basket_amount_max_1h", F.max("basket_amount").over(w3))
    .withColumn("running_basket_amount", F.sum("basket_amount").over(w4))
    .withColumn("basket_amount_percent_rank", F.percent_rank().over(w5))
)
