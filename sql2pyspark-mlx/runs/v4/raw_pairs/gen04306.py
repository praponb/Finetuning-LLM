from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country")
w2 = Window.partitionBy("ship_country").orderBy(F.col("bulk_lines").desc(), "id")
w3 = Window.orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("ship_country").orderBy("bulk_lines", "id")
w5 = Window.partitionBy("ship_country").orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "attributes",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.col("ship_to.country").alias("ship_country"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
    )
)

result = (
    unpacked
    .withColumn("ship_country_avg_bulk_lines", F.avg("bulk_lines").over(w))
    .withColumn("bulk_lines_bucket", F.ntile(5).over(w2))
    .withColumn("last_known_bulk_lines", F.last("bulk_lines", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("running_bulk_lines", F.sum("bulk_lines").over(w5))
)
