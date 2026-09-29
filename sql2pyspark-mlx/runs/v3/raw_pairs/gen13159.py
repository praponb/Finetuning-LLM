from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%ran%") & (F.col("segment") == "Corporate"))
    .groupBy("country", "city")
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
        F.sum("age").alias("total_age"),
    )
)
