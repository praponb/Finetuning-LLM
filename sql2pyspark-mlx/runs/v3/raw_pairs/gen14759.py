from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .union(df_store_sales.select("sale_date"))
    .distinct()
    .orderBy(F.col("start_date").desc())
)
