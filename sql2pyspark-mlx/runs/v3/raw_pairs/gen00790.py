from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date")
    .unionByName(df_shipments.select(F.col("delivered_date").alias("opened_date")))
)
