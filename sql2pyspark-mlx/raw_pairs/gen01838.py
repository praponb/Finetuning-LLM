from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").isNotNull())
    .select("opened_date")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date").isNotNull())
        .select(F.col("sale_date").alias("opened_date"))
    )
    .distinct()
)
