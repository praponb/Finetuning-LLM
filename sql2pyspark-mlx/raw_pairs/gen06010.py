from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select(F.lit("account").alias("record_type"), "opened_date")
    .union(
        df_shipments
        .select(F.lit("shipment").alias("record_type"), "delivered_date")
    )
    .distinct()
    .orderBy("opened_date")
)
