from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("opened_date_plus_39d", "id")
w2 = Window.partitionBy(F.month("opened_date"), "status").orderBy(F.col("opened_date_plus_39d").desc(), "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-07-04").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.date_add("opened_date", 39).alias("opened_date_plus_39d"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "opened_date",
        "opened_date_plus_39d",
        F.row_number().over(w_recent).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)
