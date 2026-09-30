from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-05-01")
    .select("status")
    .subtract(df_shipments.filter(F.col("cost").between(32, 87)).select("status"))
    .orderBy("status")
)
