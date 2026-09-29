from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("country").isin(["US", "JP"]) | F.col("supplier_name").like("Pac%"))
    .withColumn("supplier_name_upper", F.upper("supplier_name"))
)

city_summary = (
    base
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("rating").alias("rating_count"),
    )
)

result = (
    city_summary
    .select("city", "rating_count")
    .orderBy("city")
)
