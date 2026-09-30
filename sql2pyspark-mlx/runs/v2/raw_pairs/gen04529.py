from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "status",
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
)
