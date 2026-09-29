from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CHECKING"]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("source")))
    .distinct()
)
