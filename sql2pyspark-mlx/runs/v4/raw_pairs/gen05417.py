from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

joined = (
    flattened
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.email"))
)

joined_carts = (
    joined
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.age"), F.col("b.segment"), F.col("b.signup_date"))
)

result = (
    joined_carts
    .groupBy("attr_key")
    .pivot("segment", ["SMB", "Corporate"])
    .agg(F.max("age").alias("total"), F.count(F.lit(1)).alias("n"))
)
