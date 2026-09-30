from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 65, "HIGH").otherwise("LOW").alias("element_tier"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("element", 0.5).alias("element_p50"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
