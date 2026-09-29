from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "web", "partner"]))
    .select("status")
    .intersect(df_accounts.select("status"))
)
