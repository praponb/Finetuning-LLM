from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("element") >= 60, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.size(F.collect_set("currency")).alias("num_currencies_2"),
    )
)
