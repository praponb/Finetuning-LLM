from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .orderBy(F.col("launch_quarter").desc())
    .limit(3)
)
