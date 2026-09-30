from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy(F.date_trunc("week", "opened_date"), "status").orderBy("opened_date", "id")

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-05-17"))) & F.col("customer_id").isNotNull())
    .withColumn("account_type_upper", F.upper("account_type"))
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "account_type_upper",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
