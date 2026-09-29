from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("bulk_lines", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("bulk_lines").desc(), "id")
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy("bulk_lines", "id")

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id") == 3)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
    )
)

result = (
    flattened_attrs
    .withColumn("last_known_bulk_lines", F.last("bulk_lines", ignorenulls=True).over(w))
    .withColumn("bulk_lines_bucket", F.ntile(3).over(w2))
    .withColumn("customer_id_max_bulk_lines", F.max("bulk_lines").over(w3))
    .withColumn("prev_bulk_lines", F.lag("bulk_lines", 2).over(w4))
)
