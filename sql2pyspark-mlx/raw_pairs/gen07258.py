from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

result = (
    flattened
    .select(
        "id",
        "created_at",
        "element",
        F.when(F.col("element").rlike("^(VIP|WELCOME5)$"), "CORE").when(F.col("element").rlike("^(SPRING10|BUNDLE)$"), "EXTENDED").otherwise("PRIORITY").alias("element_zone"),
    )
    .orderBy(F.col("element_zone").desc())
)
