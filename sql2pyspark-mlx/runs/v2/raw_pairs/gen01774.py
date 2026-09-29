from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CHECKING") & (F.year("opened_date") == 2025))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 6))
    .withColumn("account_type_status", F.concat("account_type", F.lit(" - "), "status"))
    .orderBy(F.col("id").desc(), F.col("status").desc())
    .limit(3)
)
