from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-08-01")
    .select(
        "id",
        "status",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
)
