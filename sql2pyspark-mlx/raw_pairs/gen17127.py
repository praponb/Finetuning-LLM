from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isin([7, 13, 27]))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    flattened_attrs
    .groupBy("customer_id")
    .agg(
        F.countDistinct("gift").alias("distinct_gifts"),
        F.count("*").alias("num_rows"),
    )
)
