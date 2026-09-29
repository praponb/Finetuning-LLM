from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(46000, 59000))
    .orderBy("opened_date", "id")
    .limit(10)
)
