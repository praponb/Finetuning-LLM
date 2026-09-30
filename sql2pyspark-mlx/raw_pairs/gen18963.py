from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("element"),
    )
)

result = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "element",
        F.when(F.col("element").rlike("^(WELCOME5|VIP)$"), "PRIORITY").otherwise("GROUP_A").alias("element_zone"),
    )
    .orderBy(F.col("created_at").desc_nulls_first())
)
