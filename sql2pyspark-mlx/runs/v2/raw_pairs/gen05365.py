from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CHECKING", "LOAN"]))
    .select("status")
    .unionByName(df_shipments.select("status"))
    .orderBy("status")
)
