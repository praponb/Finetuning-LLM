from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("opened_date").desc(), "id")

filtered = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)
