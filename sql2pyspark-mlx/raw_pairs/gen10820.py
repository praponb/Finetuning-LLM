from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

cleaned = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-03-11").cast("date")) & F.col("effective_from").between("2024-03-01", "2024-12-14"))
    .select(
        "plan",
        "effective_from",
        "currency",
        "val",
        F.upper("plan").alias("plan_upper"),
        F.when(F.col("val") >= 79, "High").when(F.col("val") >= 16, "Medium").otherwise("Low").alias("val_level"),
    )
)

filtered = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-04-13").cast("date"))
    .select("*", F.add_months("effective_from", 6).alias("effective_from_plus_months"))
)

enriched = filtered.withColumn("val_cume_dist", F.cume_dist().over(w))

cleaned_price_tiers = (
    enriched
    .filter(F.col("effective_from").between("2024-06-01", "2024-06-21"))
    .select(
        "plan",
        "currency",
        "plan_upper",
        "val_level",
        "effective_from_plus_months",
        "val_cume_dist",
        F.round("val", 1).alias("val_rounded"),
    )
)

grouped = (
    cleaned_price_tiers
    .groupBy(
        F.date_trunc("MONTH", "effective_from_plus_months").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("val_cume_dist")).otherwise(0)).alias("eur_val_cume_dist"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
    )
    .filter(F.col("num_currencies") >= 1)
)

result = (
    grouped
    .select("activity_month", "currency_list", "num_currencies", "eur_val_cume_dist")
    .orderBy(F.col("num_currencies").desc_nulls_first(), F.col("activity_month").desc())
    .limit(5)
)
