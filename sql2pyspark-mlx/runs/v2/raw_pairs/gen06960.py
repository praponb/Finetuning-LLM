from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.4) & F.col("supplier_name").like("%EuroGood%"))
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
)
