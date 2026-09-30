from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") > "2024-06-16") | (F.col("supplier_id") == 4))
    .groupBy("category", "supplier_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
    .filter(F.col("globex_count") < 2)
)
