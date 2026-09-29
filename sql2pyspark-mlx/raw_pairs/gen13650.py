from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-03-29")
    .select("status")
    .unionByName(df_shipments.filter(F.year("shipped_date") == 2025).select("status"))
    .orderBy("status")
)
