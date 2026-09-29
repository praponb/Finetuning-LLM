from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(9, 14))
    .select("id", "method")
    .subtract(df_payments.filter(F.col("method") == "cash").select("id", "method"))
    .orderBy("id")
)
