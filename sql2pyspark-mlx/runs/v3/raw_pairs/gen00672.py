from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & (F.col("rating") <= 3.6))
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") < 74)
    .orderBy("city")
)
