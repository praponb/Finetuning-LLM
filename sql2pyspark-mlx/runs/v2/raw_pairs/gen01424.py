from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .filter(F.col("a.customer_id").isNotNull())
    .join(df_customers.filter(F.col("segment").isNull()).alias("c"), F.col("a.customer_id") == F.col("c.id"), "left_anti")
    .filter(F.col("a.customer_id").isNotNull())
    .select(F.col("a.id"), F.col("a.status"), F.col("a.opened_date"))
    .orderBy(F.col("opened_date").desc())
)
