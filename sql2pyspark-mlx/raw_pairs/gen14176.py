from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("val"))

base = (
    exploded
    .filter(F.col("plan").isin(["enterprise", "starter", "pro"]))
    .select("*", F.round((F.col("val") * 100), 2).alias("adjusted_val"))
)

cleaned = (
    base
    .filter(F.col("plan").isin(["enterprise", "pro"]))
    .withColumn("adjusted_val_times_val", (F.col("adjusted_val") * F.col("val")))
    .withColumn("plan_upper", F.upper("plan"))
)

remaining_keys = (
    cleaned
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.year("effective_from") == 2025)
        .select("id")
    )
)

reconciled = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "val", "adjusted_val", "adjusted_val_times_val", "plan_upper")
)
