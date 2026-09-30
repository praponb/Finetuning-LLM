from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        "status",
    )
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy("opened_month")
)
