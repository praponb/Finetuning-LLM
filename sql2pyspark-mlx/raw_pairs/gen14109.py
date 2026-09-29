from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())
w_recent = Window.partitionBy("customer_id").orderBy("opened_date_year", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("balance").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        F.year("opened_date").alias("opened_date_year"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "balance",
        "opened_date_year",
        F.avg("opened_date_year").over(w).alias("account_type_avg_opened_date_year"),
        F.percent_rank().over(w2).alias("balance_percent_rank"),
        F.avg("balance").over(w_recent).alias("balance_avg_2"),
    )
)
