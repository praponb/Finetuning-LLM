from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .filter(F.col("a.customer_id").isNotNull())
    .join(df_customers.filter(~F.col("city").isin(["Berlin", "London", "Chiang Mai"])).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.customer_id"),
    )
    .orderBy(F.col("id").desc())
)
