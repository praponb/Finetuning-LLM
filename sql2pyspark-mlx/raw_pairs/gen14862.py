from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

result = (
    flattened
    .groupBy("customer_id")
    .agg(
        F.min("created_at").alias("first_seen"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
