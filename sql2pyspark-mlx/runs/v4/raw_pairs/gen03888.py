from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("account_type_status", F.concat("account_type", F.lit(" "), "status"))
    .withColumn("abs_balance", F.abs("balance"))
    .orderBy("opened_date", "id")
    .limit(10)
)
