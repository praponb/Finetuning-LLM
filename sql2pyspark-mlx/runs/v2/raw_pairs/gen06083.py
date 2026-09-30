from pyspark.sql import functions as F

result = (
    df_stores
    .cube(F.year("opened_date").alias("opened_year"), "region")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
    )
)
