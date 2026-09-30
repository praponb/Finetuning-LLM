from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select(F.col("opened_date").alias("launch_date"))
    )
)
