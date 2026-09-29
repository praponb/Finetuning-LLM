from pyspark.sql import functions as F

by_dimension = (
    df_orders
    .alias("a")
    .join(df_customers.alias("dim"), F.col("a.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "city"], ["country"], []],
        F.col("dim.country").alias("country"),
        F.col("dim.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.amount")).alias("total_amount"),
    )
)

result = (
    by_dimension
    .select("country", "city", "num_rows", "total_amount")
    .orderBy(F.col("country").desc_nulls_last())
)
