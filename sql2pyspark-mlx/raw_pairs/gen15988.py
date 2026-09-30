from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.col("country") == "JP") & ~F.col("country").isin(["DE", "JP"]))
)

country_group_summary = (
    recent_suppliers
    .groupBy(
        F.when(F.col("country").isin(["JP", "SG"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"))
)

result = (
    country_group_summary
    .filter(F.col("singapore_count") >= 5)
    .select("country_group", "singapore_count")
    .orderBy(F.col("singapore_count").desc())
)
