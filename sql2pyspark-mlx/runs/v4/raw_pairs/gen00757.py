from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & ((F.year("opened_date") == 2025) | F.col("status").isin(["FROZEN", "CLOSED", "OPEN"])))
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.when(F.col("status") == "CLOSED", 1).otherwise(0).alias("is_closed"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
