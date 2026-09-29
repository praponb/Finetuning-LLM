from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("key").orderBy(F.col("is_spring10_flag").desc(), "id", "key")
w2 = Window.partitionBy("key").orderBy("is_spring10_flag", "id", "key")
w3 = Window.partitionBy("key").orderBy("is_spring10_flag", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("is_spring10_flag").desc(), "id", "key")

exploded = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("key", "attr_value"))
)

filtered = (
    exploded
    .filter(F.col("key").isNotNull() & F.col("key").isin(["weight_kg", "gift", "device"]))
    .withColumn("clean_key", F.trim("key"))
    .withColumn("is_spring10", F.array_contains("coupon_codes", "SPRING10"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("OTHER")))
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "key",
        "attr_value",
        "clean_key",
        "is_spring10",
        "resolved_key",
        F.lower("key").alias("key_lower"),
    )
)

filtered_carts = (
    cleaned
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("is_spring10"), "Yes").otherwise("No").alias("is_spring10_label"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key_2"),
    )
)

valid = (
    filtered_carts
    .filter(F.col("key").isNotNull())
    .withColumn("key_filled", F.coalesce("key", F.lit("Unknown")))
)

valid_carts = (
    valid
    .filter(F.array_contains("coupon_codes", "SPRING10") & F.col("key").isNotNull())
    .select("*", F.col("is_spring10").cast("int").alias("is_spring10_flag"))
)

result = (
    valid_carts
    .filter(F.col("is_spring10"))
    .select(
        "id",
        "coupon_codes",
        "key",
        "key_lower",
        "resolved_key_2",
        "is_spring10_flag",
        F.row_number().over(w_running).alias("row_seq"),
        F.lag("is_spring10_flag", 1).over(w2).alias("prev_is_spring10_flag"),
        F.row_number().over(w_running).alias("row_seq_2"),
        F.sum("is_spring10_flag").over(w3).alias("running_is_spring10_flag"),
        F.row_number().over(w4).alias("row_seq_3"),
    )
)
