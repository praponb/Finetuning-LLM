from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city").isin(["London", "Tokyo"]) & F.col("supplier_name").isNotNull()) | (F.col("city") == "Singapore"))
    .agg(
        F.sum("rating").alias("total_rating"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
    .orderBy("total_rating")
)
