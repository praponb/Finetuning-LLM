from pyspark.sql import functions as F

result = (
    df_products
    .filter(((F.size("tags") > 2) & (F.col("brand") == "Initech")) | F.col("product_name").like("Run%"))
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.max("cost").alias("max_cost"),
    )
)
