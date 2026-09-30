from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("code"))

result = (
    lines
    .select(
        "id",
        "created_at",
        "code",
        F.when(F.col("code").rlike("^(SPRING10|BUNDLE)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("code_zone"),
    )
    .orderBy("id")
)
