from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("product_name").like("%Cance%"))
    .select(
        "id",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("price") / F.col("cost")), 2).alias("margin"),
        F.add_months("launch_date", 6).alias("launch_date_plus_months"),
    )
)

result = (
    base
    .withColumn("cost_tier", F.when(F.col("cost") >= 690, "HIGH").otherwise("LOW"))
    .rollup("cost_tier")
    .agg(
        F.count("*").alias("event_count"),
        F.round(F.avg("margin"), 2).alias("avg_margin"),
        F.max(F.when(F.col("brand") == "Stark", F.col("margin"))).alias("stark_max_margin"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("cost_per_acme"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("cost")).otherwise(0)).alias("stark_cost"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("cost_per_acme").desc_nulls_first())
)
