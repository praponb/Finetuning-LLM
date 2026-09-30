from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .select("status")
    .intersect(df_shipments.filter(F.col("cost") < 54).select("status"))
    .orderBy("status")
)
