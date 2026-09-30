from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNull() & (F.col("customer_id") == 16))
    .select(
        "id",
        "opened_date",
        "customer_id",
        "status",
        F.round("balance", 2).alias("balance_rounded"),
        (F.datediff(F.current_date(), "opened_date") / 365.25).alias("years_since_opened_date"),
    )
    .orderBy("customer_id")
)
