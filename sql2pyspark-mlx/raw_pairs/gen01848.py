from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .select(
        "id",
        "status",
        "balance",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
    )
)
