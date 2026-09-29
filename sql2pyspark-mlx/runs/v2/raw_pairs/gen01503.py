from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "opened_date",
        "customer_id",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
    .orderBy(F.col("opened_date_label").desc())
)
