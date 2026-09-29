from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.filter(F.month("opened_date").isin([3, 6, 8])).alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.age"), F.col("c.email"))
)
