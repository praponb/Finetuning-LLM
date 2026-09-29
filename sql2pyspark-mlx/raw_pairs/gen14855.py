from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("element"))

filtered = (
    lines
    .filter(F.col("created_at") >= F.lit("2025-03-02 02:00:00").cast("timestamp"))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM-dd"))
    .withColumn("resolved_element", F.coalesce("element", F.lit("UNKNOWN")))
)

cleaned = (
    filtered
    .filter(F.col("created_at") >= F.to_date(F.lit("2025-03-01")))
    .select(
        "id",
        "created_at",
        "element",
        F.trim("element").alias("clean_element"),
        F.when(F.col("element") == "VIP", 1).otherwise(0).alias("is_vip"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("element").isNotNull() & F.hour("created_at").between(10, 16))
    .select(
        "id",
        "created_at",
        "clean_element",
        F.when(F.col("element").isin(["VIP", "SPRING10"]), "Tier 1").otherwise("Other").alias("element_group"),
        F.date_format("created_at", "yyyy-MM-dd").alias("created_at_label"),
        F.coalesce("element", F.lit("OTHER")).alias("resolved_element"),
    )
)

result = (
    cleaned_carts
    .rollup(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
