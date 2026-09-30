from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 57000)
    .select("status")
    .intersect(df_shipments.select("status"))
    .orderBy("status")
)
