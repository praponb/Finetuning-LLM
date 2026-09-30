from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Sia%"))
    .withColumn("country_group", F.when(F.col("country").isin(["US", "JP"]), "Group A").otherwise("Other"))
)

country_summary = (
    valid_suppliers
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
)

result = (
    country_summary
    .filter(F.col("pct_de") > 67)
    .select("country", "pct_de")
)
