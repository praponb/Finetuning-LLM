from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("coupon_codes").alias("code"))

result = (
    lines
    .select(
        "code",
        F.when(F.col("code").rlike("^(SPRING10|VIP)$"), "OTHER_ZONE").when(F.col("code").rlike("^(BUNDLE|WELCOME5)$"), "PRIORITY").otherwise("GROUP_B").alias("code_zone"),
    )
    .orderBy(F.col("code").desc_nulls_first(), "code_zone")
)
