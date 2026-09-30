from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
    .select("status")
    .unionByName(df_payments.filter(F.hour("paid_at").between(11, 16)).select("status"))
    .distinct()
    .orderBy("status")
)
