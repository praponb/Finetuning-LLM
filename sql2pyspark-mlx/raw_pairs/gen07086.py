from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "created_at", "attr_value")
    .orderBy(F.col("created_at").asc_nulls_last())
)
