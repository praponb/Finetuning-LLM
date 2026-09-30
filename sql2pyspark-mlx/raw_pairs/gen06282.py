from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("status_length").desc(), "id")
w2 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("status_length").asc_nulls_first())

base = (
    df_accounts
    .filter(F.col("balance") > 28000)
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
        F.length("status").alias("status_length"),
    )
)

result = (
    base
    .filter(F.col("opened_date") > "2024-07-01")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "account_type_title",
        F.ntile(10).over(w).alias("status_length_bucket"),
        F.percent_rank().over(w2).alias("status_length_percent_rank"),
    )
)
