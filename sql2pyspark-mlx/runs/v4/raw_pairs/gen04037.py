from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([7, 10, 11]))
    .select("opened_date")
    .union(df_shipments.filter(F.col("cost") >= 80).select("delivered_date"))
    .orderBy("opened_date")
)
