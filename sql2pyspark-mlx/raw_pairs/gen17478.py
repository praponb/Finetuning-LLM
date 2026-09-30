from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

remaining_keys = (
    lines
    .select("plan", "id")
    .intersect(
        df_price_tiers
        .filter(F.col("plan").isin(["pro", "starter", "enterprise"]))
        .select("plan", "id")
    )
)

verified = (
    lines
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(~F.col("currency").isin(["THB", "EUR"]))
    .withColumn("is_enterprise", F.when(F.col("plan") == "enterprise", 1).otherwise(0))
)

filtered = (
    base
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .select(
        "*",
        F.substring("currency", 1, 4).alias("currency_prefix"),
        F.when(F.col("is_enterprise") >= 86, "High").when(F.col("is_enterprise") >= 40, "Medium").otherwise("Low").alias("is_enterprise_band"),
    )
)

result = (
    filtered
    .groupingSets([["plan", "currency"], ["plan"], []], "plan", "currency")
    .agg(
        F.sum("val").alias("total_val"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("is_enterprise"))).alias("eur_avg_is_enterprise"),
        F.sum(F.when(F.col("plan") == "pro", F.col("val")).otherwise(0)).alias("pro_val"),
        F.grouping("plan").alias("grouping_level"),
    )
    .orderBy(F.col("pro_val").desc_nulls_last())
)
