from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("direct")).alias("utm_source_or_default"),
        F.size("attributes").alias("num_attributes"),
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
    )
)
