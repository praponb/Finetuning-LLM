from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_first())

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-06-03").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_accounts = (
    latest
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

matched_keys = (
    latest_accounts
    .select("customer_id", "status")
    .intersectAll(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("customer_id", "status")
    )
)

kept_rows = (
    latest_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

wide = (
    kept_rows
    .groupBy("status")
    .pivot("account_type", ["CHECKING", "LOAN"])
    .agg(F.avg("balance").alias("amount"))
    .select("status", F.col("CHECKING").alias("checking"), F.col("LOAN").alias("loan"))
)

result = (
    wide
    .select("status", "checking", "loan")
    .orderBy(F.col("loan").desc_nulls_first())
)
