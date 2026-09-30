from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("num_items").desc())
w2 = Window.orderBy("num_attributes", "id")
w3 = Window.orderBy("num_items", "id").rowsBetween(-5, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.col("attributes").getItem("device").alias("device"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("items").alias("num_items"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    unpacked
    .withColumn("num_items_cume_dist", F.cume_dist().over(w))
    .withColumn("prev_bulk_lines", F.lag("bulk_lines", 2).over(w2))
    .withColumn("bulk_lines_avg_5", F.avg("bulk_lines").over(w3))
)
