from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("rating").alias("unique_citys"),
    )
)
