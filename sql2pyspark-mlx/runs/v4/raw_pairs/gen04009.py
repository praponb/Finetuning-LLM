from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(df_shipments.select("status"))
    .distinct()
    .orderBy("status")
)
