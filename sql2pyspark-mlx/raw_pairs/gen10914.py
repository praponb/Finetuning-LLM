from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]))
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.total_balance") >= 284000)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("cs.account_type_values"),
        F.col("cs.total_balance"),
    )
    .orderBy(F.col("total_balance").desc(), "id")
    .limit(20)
)
