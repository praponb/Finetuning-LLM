from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type")

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-07-06").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
)

scored = (
    valid
    .withColumn("cumulative_balance", F.sum("balance").over(w))
    .withColumn("account_type_avg_balance", F.avg("balance").over(w2))
)

filtered = (
    scored
    .filter(F.col("opened_date") >= F.lit("2024-04-09").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "opened_date_label",
        "cumulative_balance",
        "account_type_avg_balance",
        F.round((F.col("cumulative_balance") * 1.15), 2).alias("adjusted_cumulative_balance"),
        (F.col("cumulative_balance") / F.nullif("balance", F.lit(0))).alias("cumulative_balance_per_balance"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.credit_limit"), F.col("c.email"), F.col("c.age"))
)

result = (
    with_dims
    .groupBy(
        "customer_id",
        F.when(F.col("account_type_avg_balance") >= 65172, "PREMIUM").otherwise("STANDARD").alias("account_type_avg_balance_tier"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max(F.when(F.col("account_type") == "LOAN", F.col("credit_limit"))).alias("loan_max_credit_limit"),
        F.avg(F.when(F.col("account_type") == "SAVINGS", F.col("cumulative_balance"))).alias("savings_avg_cumulative_balance"),
    )
)
