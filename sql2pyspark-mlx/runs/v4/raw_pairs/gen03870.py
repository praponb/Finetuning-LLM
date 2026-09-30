from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .union(df_accounts.filter(F.col("status") == "CLOSED").select("opened_date"))
    .distinct()
    .orderBy("delivered_date")
)
