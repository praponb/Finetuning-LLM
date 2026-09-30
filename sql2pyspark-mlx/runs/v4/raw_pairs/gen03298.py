from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.hour("created_at").between(9, 16))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)
