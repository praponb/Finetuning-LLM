from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "DHL"]))
    .select(F.col("shipped_date").alias("event_date"))
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isin(["CREDIT", "LOAN"]))
        .select(F.col("opened_date").alias("event_date"))
    )
    .distinct()
)
