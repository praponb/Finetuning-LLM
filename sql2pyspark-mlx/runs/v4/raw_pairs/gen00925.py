from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("status_upper", F.upper("status"))
    .withColumn("balance_rounded", F.round("balance", 0))
    .orderBy("customer_id", "id")
    .limit(50)
)
