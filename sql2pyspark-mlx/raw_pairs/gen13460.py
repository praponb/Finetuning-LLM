from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 67, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
    )
    .filter(F.col("basic_events") < 1000)
    .orderBy(F.col("basic_events").asc_nulls_last())
)
