from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("status_filled", F.coalesce("status", F.lit("Unknown")))
    .withColumn("account_type_status", F.concat("account_type", F.lit(", "), "status"))
)
