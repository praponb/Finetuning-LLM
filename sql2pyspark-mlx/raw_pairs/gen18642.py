from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-24").cast("date")) & F.col("status").isNotNull())
    .withColumn("opened_date_day", F.dayofmonth("opened_date"))
    .withColumn("status_lower", F.lower("status"))
)

result = (
    cleaned
    .select("id", "account_type")
    .intersect(
        df_accounts
        .filter(F.col("balance") >= 72000)
        .select("id", "account_type")
    )
)
