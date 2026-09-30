from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

result = (
    lines
    .select(
        "id",
        "created_at",
        "tag",
        F.when(F.col("tag").rlike("^(VIP|SPRING10)$"), "PRIORITY").when(F.col("tag").rlike("^(BUNDLE|WELCOME5)$"), "GROUP_B").otherwise("CORE").alias("tag_zone"),
    )
    .orderBy(F.col("created_at").asc_nulls_last())
)
