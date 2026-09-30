from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("cost")).otherwise(0)).alias("globex_cost"),
        F.count("cost").alias("cost_count"),
    )
    .orderBy(F.col("launch_quarter").desc())
)
