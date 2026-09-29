from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
    )
)
