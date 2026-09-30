from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(df_shipments.filter(F.col("cost").between(27, 94)).select("status"))
    .distinct()
)
