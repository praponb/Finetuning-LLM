from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(F.col("orders.channel").isin(["partner", "mobile"]))
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("orders.status")))).alias("status_values"),
        F.sum(F.col("orders.amount")).alias("total_amount"),
    )
    .filter(F.col("total_amount") >= 5300)
    .orderBy("signup_quarter")
)
