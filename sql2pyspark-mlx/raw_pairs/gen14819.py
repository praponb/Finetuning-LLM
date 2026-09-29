from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") == 12532) & (F.col("opened_date") <= "2025-06-07"))
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)
