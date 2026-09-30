from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 15) & F.col("customer_id").isNotNull() & F.col("account_type").isin(["SAVINGS", "CHECKING"]))
    .select(
        "customer_id",
        "status",
        "opened_date",
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
    )
    .orderBy("is_closed")
)
