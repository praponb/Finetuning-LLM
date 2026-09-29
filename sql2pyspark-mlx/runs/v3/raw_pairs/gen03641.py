from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-07-01", "2025-03-01") & F.col("store_name").isNotNull() & (F.col("sqft") == 12716))
    .select(
        "id",
        "city",
        "region",
        "store_name",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)
