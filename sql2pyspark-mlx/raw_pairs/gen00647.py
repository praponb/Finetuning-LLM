from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy("num_items", "id")
w3 = Window.orderBy("num_attributes", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("num_attributes", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_carts
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("attributes")["device"].alias("device"),
        F.size("items").alias("num_items"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "coupon_codes",
        "device",
        "num_items",
        "num_attributes",
        F.stddev_samp("num_attributes").over(w).alias("customer_id_stddev_samp_num_attributes"),
        F.lead("num_items", 1).over(w2).alias("next_num_items"),
        F.avg("num_attributes").over(w3).alias("num_attributes_avg_5"),
        F.sum("num_attributes").over(w4).alias("running_num_attributes"),
    )
)
