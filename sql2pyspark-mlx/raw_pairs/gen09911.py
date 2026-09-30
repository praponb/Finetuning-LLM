from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("direct")).alias("utm_source_or_default"),
    )
)
