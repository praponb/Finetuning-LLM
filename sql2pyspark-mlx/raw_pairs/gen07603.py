from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").eqNullSafe(F.lit("OPEN")))
    .withColumn("account_type_status_key", F.concat_ws("-", "account_type", "status"))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("num_account_types") < 1000)
    .orderBy(F.col("year_start").asc_nulls_first())
)
