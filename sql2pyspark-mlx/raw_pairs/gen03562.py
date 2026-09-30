from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").isNotNull())
    .select("opened_date")
    .unionByName(
        df_orders
        .filter(F.col("order_date").isNotNull())
        .select(F.col("order_date").alias("opened_date"))
    )
)
