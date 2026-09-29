from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("units") >= 24)
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

joined = (
    base
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.launch_date"), F.col("d.category"))
)

enriched = (
    joined
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.store_name"), F.col("b.opened_date"))
)

result = (
    enriched
    .select("id", "category", F.col("revenue").alias("REVENUE_VAL"), "revenue_per_unit")
    .unpivot(
        ["id", "category"],
        ["REVENUE_VAL", "revenue_per_unit"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
