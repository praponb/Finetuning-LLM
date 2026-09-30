from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store"]))
    .select("status")
    .intersect(df_accounts.filter(F.col("account_type") == "LOAN").select("status"))
    .orderBy("status")
)
