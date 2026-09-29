from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("opened_date") >= "2023-10-01")
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    df_customers.alias("customers")
    .join(accounts_by_customer.alias("AccountsByCustomer"), F.col("customers.id") == F.col("AccountsByCustomer.customer_id"), "inner")
    .select(
        F.col("customers.name"),
        F.col("customers.email"),
        F.col("AccountsByCustomer.status_values"),
        F.col("AccountsByCustomer.account_type_values"),
    )
    .orderBy("status_values")
)
