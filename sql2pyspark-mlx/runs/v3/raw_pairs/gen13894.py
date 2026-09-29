from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 390)
    .select(F.col("launch_date").alias("event_date"))
    .intersect(
        df_accounts
        .filter(F.col("opened_date").between("2024-01-19", "2024-11-01"))
        .select(F.col("opened_date").alias("event_date"))
    )
)
