from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(300, 510))
    .groupBy(F.year("launch_date").alias("launch_year"), "supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_count"))
    .orderBy("launch_year")
)
