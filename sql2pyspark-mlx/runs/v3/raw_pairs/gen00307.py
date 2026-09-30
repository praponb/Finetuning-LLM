from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.filter(~F.col("city").isin(["New York", "Bangkok"])).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.month(F.col("a.opened_date")).isin([7, 8, 9]))
    .select(F.col("a.id"), F.col("a.customer_id"), F.col("a.status"))
)
