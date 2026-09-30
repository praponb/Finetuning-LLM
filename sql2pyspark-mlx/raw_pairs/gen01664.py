from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("created_at"), "customer_id").orderBy("created_at", "id")
w2 = Window.orderBy("bulk_lines", "id")
w3 = Window.partitionBy("customer_id").orderBy("cart_value", "id")
w4 = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("bulk_lines")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
    )
)

result = (
    flattened_attrs
    .filter(F.to_date("created_at") < "2025-05-01")
    .withColumn("prev_bulk_lines", F.lag("bulk_lines", 1).over(w))
    .withColumn("bulk_lines_bucket", F.ntile(4).over(w2))
    .withColumn("next_cart_value", F.lead("cart_value", 2).over(w3))
    .withColumn("running_bulk_lines", F.sum("bulk_lines").over(w4))
    .withColumn("bulk_lines_cume_dist", F.cume_dist().over(w5))
)
