from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes").getItem("device").alias("device"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.size("items").alias("num_items"),
    )
)
