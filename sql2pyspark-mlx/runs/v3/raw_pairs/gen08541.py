from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([8, 18, 23]))
    .select("status")
    .intersect(df_shipments.select("status"))
    .orderBy("status")
)
