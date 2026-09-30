from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.count("*").alias("num_products"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("num_products") > 4)
    .select("launch_year", "num_products", "cost_range")
    .orderBy("cost_range", "launch_year")
    .limit(50)
)
