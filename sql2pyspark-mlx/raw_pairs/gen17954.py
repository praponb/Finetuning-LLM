from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["gift"].alias("gift"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.country").alias("ship_country"),
        F.size("attributes").alias("num_attributes"),
    )
)
