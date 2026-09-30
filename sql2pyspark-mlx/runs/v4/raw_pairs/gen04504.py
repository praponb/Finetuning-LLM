from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
)
