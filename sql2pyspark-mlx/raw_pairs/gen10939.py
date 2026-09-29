from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

valid = (
    lines
    .filter((F.col("created_at") > F.lit("2025-03-01 20:00:00").cast("timestamp")) & F.col("value").isNotNull())
    .withColumn("value_filled", F.coalesce("value", F.lit("Unknown")))
)

base = (
    valid
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 13:00:00"))) & F.col("value").isNotNull())
    .select(
        "*",
        F.dayofweek("created_at").alias("created_at_dayofweek"),
        F.trim("value").alias("clean_value"),
    )
)

filtered = (
    base
    .filter(F.col("value") == "VIP")
    .select(
        "*",
        F.coalesce("created_at_dayofweek", F.lit(0)).alias("created_at_dayofweek_filled"),
        F.when(F.col("value").isin(["WELCOME5", "SPRING10"]), "Group A").otherwise("Other").alias("value_group"),
    )
)

base_carts = (
    filtered
    .filter(F.col("value").isNotNull())
    .withColumn("created_at_dayofweek_filled_minus_created_at_dayofweek", (F.col("created_at_dayofweek_filled") - F.col("created_at_dayofweek")))
)

result = (
    base_carts
    .select(
        "id",
        "created_at",
        "value",
        "value_filled",
        "created_at_dayofweek",
        "clean_value",
        "created_at_dayofweek_filled",
        "value_group",
        "created_at_dayofweek_filled_minus_created_at_dayofweek",
    )
    .orderBy("created_at_dayofweek_filled")
)
