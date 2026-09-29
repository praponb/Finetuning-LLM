from pyspark.sql import functions as F

filtered_products = df_products.filter(F.col("supplier_id").isNotNull())

launch_year_summary = (
    filtered_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
    )
)

result = (
    launch_year_summary
    .filter(F.col("electronics_count") >= 5)
    .orderBy("electronics_count")
)
