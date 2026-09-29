from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-07-18", "2025-06-01") & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "sqft",
        F.when(F.col("region") == "East", 1).otherwise(0).alias("is_east"),
        F.quarter("opened_date").alias("opened_date_quarter"),
    )
)
