from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2025)
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(df_accounts.select("opened_date", F.lit("account").alias("source")))
)
