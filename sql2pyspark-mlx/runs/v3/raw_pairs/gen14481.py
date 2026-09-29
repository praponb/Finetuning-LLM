from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 5)
)
