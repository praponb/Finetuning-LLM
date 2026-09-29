from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("source"), "shipped_date")
    .union(df_accounts.select(F.lit("account").alias("source"), "opened_date"))
    .distinct()
    .orderBy("source")
)
