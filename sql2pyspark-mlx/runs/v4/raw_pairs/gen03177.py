from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc(), "id")
w_recent_2 = Window.partitionBy("customer_id", "account_type")
w3 = Window.partitionBy("customer_id").orderBy("opened_date", "id")

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-07-07").cast("date")) & F.col("status").isNotNull() & (F.col("customer_id") == 12))
    .select(
        "*",
        F.concat_ws("/", "status", "account_type").alias("status_account_type_key"),
    )
)

result = (
    filtered
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("customer_id_sum_balance", F.sum("balance").over(w_recent_2))
    .withColumn("prev_balance", F.lag("balance", 1).over(w3))
)
