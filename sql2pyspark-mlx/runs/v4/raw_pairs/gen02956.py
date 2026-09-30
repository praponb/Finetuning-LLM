from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
    )
)
