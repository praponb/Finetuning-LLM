from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-17").cast("date")) & (F.year("opened_date") == 2025))
    .withColumn("balance_int", F.col("balance").cast("int"))
)

result = (
    base
    .select("account_type", "status")
    .intersect(
        df_accounts
        .filter(F.col("opened_date") > "2025-08-18")
        .select("account_type", "status")
    )
)
