from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "element",
        F.when(F.col("element").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_A").when(F.col("element").rlike("^(SPRING10|VIP)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("element_zone"),
    )
)
