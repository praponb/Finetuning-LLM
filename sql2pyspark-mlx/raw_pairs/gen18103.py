from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("num_items").desc(), "id")
w3 = Window.partitionBy("ship_country", "customer_id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("unknown")).alias("device_or_default"),
        F.size("items").alias("num_items"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("ship_country").isin(["SG", "TH", "DE"]))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "device_or_default",
        "num_items",
        "utm_source",
        F.avg("num_items").over(w).alias("num_items_avg_2"),
        F.row_number().over(w2).alias("seq_no"),
        F.min("num_items").over(w3).alias("ship_country_min_num_items"),
    )
)
