from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-06-17").cast("date")) & F.col("status").isNotNull() & F.col("customer_id").isin([5, 32, 34]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.length("account_type").alias("account_type_length"),
    )
)

result = (
    cleaned
    .select("status", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("status", "customer_id")
    )
)
