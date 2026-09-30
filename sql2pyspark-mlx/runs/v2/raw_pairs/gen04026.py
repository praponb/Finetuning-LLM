from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(48000, 73000))
    .select("status")
    .distinct()
    .orderBy("status")
)
