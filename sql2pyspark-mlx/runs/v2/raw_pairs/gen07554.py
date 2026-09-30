from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .pivot("status", ["OPEN", "FROZEN"])
    .agg(F.sum("balance"))
    .orderBy("opened_year")
)
