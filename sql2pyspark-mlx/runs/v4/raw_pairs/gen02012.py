from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

result = (
    exploded
    .select(
        "id",
        "created_at",
        "value",
        F.when(F.col("value").rlike("^(VIP|BUNDLE)$"), "CORE").when(F.col("value").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("value_zone"),
    )
    .orderBy(F.col("value_zone").desc())
)
