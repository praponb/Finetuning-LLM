from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("supplier_name").like("%GmbH%"))

summary = (
    recent_suppliers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
)

result = summary.orderBy("pct_de")
