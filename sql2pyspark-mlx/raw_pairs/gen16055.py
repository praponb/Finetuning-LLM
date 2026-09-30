from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").isNotNull())
    .select(
        F.col("opened_date").alias("event_date"),
        F.lit("account").alias("record_type"),
    )
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date").isNotNull())
        .select(
        F.col("sale_date").alias("event_date"),
        F.lit("store_sale").alias("record_type"),
    )
    )
)
