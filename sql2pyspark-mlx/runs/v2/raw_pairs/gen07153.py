from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "LOAN"]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "Kerry")
        .select("status", F.lit("shipment").alias("source"))
    )
)
