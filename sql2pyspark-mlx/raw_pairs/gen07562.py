from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("coupon_codes_count", "id", "k")

exploded = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("k", "attr_value"))
)

filtered = (
    exploded
    .filter(F.col("k").isNotNull() & F.col("k").isin(["weight_kg", "device"]))
    .withColumn("is_welcome5", F.array_contains("coupon_codes", "WELCOME5"))
)

filtered_carts = (
    filtered
    .filter(F.col("k").isNotNull())
    .withColumn("coupon_codes_count", F.size("coupon_codes"))
)

latest = (
    filtered_carts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "coupon_codes",
        "k",
        "attr_value",
        "is_welcome5",
        "coupon_codes_count",
    )
)

cleaned = (
    latest
    .filter(F.col("k").isin(["device", "gift", "weight_kg"]))
    .select(
        "id",
        "k",
        "attr_value",
        "is_welcome5",
        "coupon_codes_count",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
        F.coalesce("k", F.lit("N/A")).alias("k_filled"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled_2"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("coupon_codes_count") >= 80, "HIGH").otherwise("LOW").alias("coupon_codes_count_tier"),
    )
    .agg(
        F.sum(F.when(F.col("k") == "weight_kg", F.col("coupon_codes_count")).otherwise(0)).alias("weight_kg_coupon_codes_count"),
        F.percentile_approx("coupon_codes_count", [0.25, 0.5, 0.75]).alias("coupon_codes_count_quartiles"),
    )
    .orderBy("coupon_codes_count_tier")
)
