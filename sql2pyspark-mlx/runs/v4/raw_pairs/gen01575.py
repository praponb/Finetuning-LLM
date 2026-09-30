from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2024-04-21").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.length("account_type").alias("account_type_length"))
)

result = (
    base
    .select("account_type")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") >= "2024-07-01")
        .select("account_type")
    )
)
