from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-07-18").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.substring("account_type", 1, 4).alias("account_type_prefix"))
)

result = (
    valid
    .select("account_type")
    .intersect(
        df_accounts
        .filter(F.col("status") != "FROZEN")
        .select("account_type")
    )
)
