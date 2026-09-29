from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") >= 2024) & (F.col("store_id").isin([22, 31]) | F.col("order_date").between("2023-11-01", "2025-09-30")))
    .groupBy("status")
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
    )
    .orderBy(F.col("cnt").desc())
)
